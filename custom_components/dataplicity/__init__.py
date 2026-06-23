import inspect
import logging
from threading import Event, Thread

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant
from homeassistant.requirements import async_process_requirements
from homeassistant.util import package

from . import utils

DOMAIN = "dataplicity"
_LOGGER = logging.getLogger(__name__)
_WATCHDOG_INTERVAL = 30
_RESTART_DELAY = 10


def _close_client(client):
    m2m_client = getattr(getattr(client, "m2m", None), "m2m_client", None)

    try:
        client.exit()
    except Exception:
        _LOGGER.exception("Failed to stop Dataplicity client")

    try:
        client.port_forward.close_event.set()
    except Exception:
        _LOGGER.debug("Failed to close Dataplicity port forwards", exc_info=True)

    try:
        client.m2m.close()
    except Exception:
        _LOGGER.debug("Failed to close Dataplicity m2m client", exc_info=True)

    if m2m_client is not None and m2m_client.is_alive():
        try:
            m2m_client.join(timeout=10)
        except RuntimeError:
            _LOGGER.debug("Could not join Dataplicity m2m client", exc_info=True)
        if m2m_client.is_alive():
            _LOGGER.warning("Dataplicity m2m client did not stop within timeout")


async def async_setup(hass: HomeAssistant, hass_config: dict):
    real_install = package.install_package

    def fake_install(pkg: str, *args, **kwargs):
        if pkg == "dataplicity==0.5.13":
            return utils.install_package(pkg, *args, **kwargs)
        return real_install(pkg, *args, **kwargs)

    try:
        package.install_package = fake_install

        # 0.4.40 talks to the legacy api.dataplicity.com and is rejected with
        # HTTP 403 for devices provisioned via the new device-gateway API;
        # 0.5.13 works with the new provisioning.
        await async_process_requirements(hass, DOMAIN, ["dataplicity==0.5.13"])

        # fix Python 3.11 support
        if not hasattr(inspect, "getargspec"):

            def getargspec(*args):
                spec = inspect.getfullargspec(*args)
                return spec.args, spec.varargs, spec.varkw, spec.defaults

            inspect.getargspec = getargspec

        return True
    except:
        return False
    finally:
        package.install_package = real_install


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    # fix https://github.com/AlexxIT/Dataplicity/issues/29
    Client = await hass.async_add_executor_job(utils.import_client)

    stop_event = Event()
    state = {
        "agent_thread": None,
        "client": None,
        "stop_event": stop_event,
        "watchdog_thread": None,
    }

    def run_agent():
        while not stop_event.is_set():
            client = None
            agent_thread = None
            try:
                client = Client(serial=entry.data["serial"], auth_token=entry.data["auth"])
                state["client"] = client
                # replace default 80 port to Hass port (usual 8123)
                client.port_forward.add_service("web", hass.config.api.port)
                agent_thread = Thread(
                    name=f"{DOMAIN}-agent",
                    target=client.run_forever,
                    daemon=True,
                )
                state["agent_thread"] = agent_thread
                agent_thread.start()
                _LOGGER.info("Dataplicity client started")

                while not stop_event.wait(_WATCHDOG_INTERVAL):
                    if not agent_thread.is_alive():
                        _LOGGER.warning("Dataplicity agent thread stopped; restarting")
                        break

                    m2m_client = getattr(getattr(client, "m2m", None), "m2m_client", None)
                    if m2m_client is not None and not m2m_client.is_alive():
                        _LOGGER.warning("Dataplicity m2m thread stopped; restarting")
                        break
                else:
                    continue
            except Exception:
                _LOGGER.exception("Dataplicity client crashed; restarting")
            finally:
                if client is not None:
                    _close_client(client)
                if agent_thread is not None and agent_thread.is_alive():
                    agent_thread.join(timeout=10)
                if state.get("client") is client:
                    state["client"] = None
                if state.get("agent_thread") is agent_thread:
                    state["agent_thread"] = None

            if stop_event.wait(_RESTART_DELAY):
                break

    hass.data[DOMAIN] = state
    watchdog_thread = Thread(name=f"{DOMAIN}-watchdog", target=run_agent, daemon=True)
    state["watchdog_thread"] = watchdog_thread
    watchdog_thread.start()

    async def hass_stop(event):
        stop_event.set()
        if client := state.get("client"):
            _close_client(client)

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, hass_stop)

    await utils.fix_middleware(hass)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry):
    state = hass.data.pop(DOMAIN, None)
    if state is None:
        return True
    state["stop_event"].set()

    def stop_threads():
        if client := state.get("client"):
            _close_client(client)

        for key in ("agent_thread", "watchdog_thread"):
            thread = state.get(key)
            if thread is not None and thread.is_alive():
                thread.join(timeout=10)

    await hass.async_add_executor_job(stop_threads)
    return True
