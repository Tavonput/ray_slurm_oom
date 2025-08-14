import ray
import time


@ray.remote
class Flag:
    """ Flag

    Actor that just keeps track of a flag. Note that this should not eat up resources as it should (according the ray)
    not be scheduled if idle.
    """
    def __init__(self) -> None:
        self.flag = False


    def set(self) -> None:
        self.flag = True


    def state(self) -> bool:
        return self.flag


class RemoteFlag:
    """ Remote Flag

    Wrapper around the Flag actor. All calls to the Flag actor are blocking. Also provides a helper function for
    polling the flag.
    """
    def __init__(self) -> None:
        self.flag_actor = Flag.remote()
        self.state()


    def set(self) -> None:
        ray.get(self.flag_actor.set.remote())  # type: ignore


    def state(self) -> bool:
        return ray.get(self.flag_actor.state.remote())  # type: ignore


    def poll(self, timeout: float, time_int: float = 0.1) -> bool:
        """ Poll the flag. Returns False if timeout is reached. """
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.state() == True:
                return True
            time.sleep(time_int)

        return False
