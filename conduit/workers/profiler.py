import cProfile, pstats
import logging
from typing import Union, List, Callable, Optional, Tuple, Any

DEFAULT_AMOUNT = 20


logger = logging.getLogger("profiler")
outfile = open('profile.log', 'a')


class ProfileFunc:
    def __init__(self, func: Callable, sort_stats_by: str):
        self.func: Callable = func
        self.profile_runs: List[cProfile.Profile] = []
        self.sort_stats_by: str = sort_stats_by

    def __call__(self, *args, **kwargs) -> Tuple[Any, pstats.Stats]:
        pr = cProfile.Profile()
        pr.enable()  # this is the profiling section
        retval = self.func(*args, **kwargs)
        pr.disable()

        self.profile_runs.append(pr)
        ps = pstats.Stats(*self.profile_runs, stream=outfile).sort_stats(self.sort_stats_by)
        return retval, ps


def cumulative_profiler(amount_of_times: Optional[Union[Callable, int]] = DEFAULT_AMOUNT,
                        sort_stats_by: str='tottime') -> Callable:

    def real_decorator(func: Callable):
        def wrapper(*args, **kwargs):
            nonlocal func, amount_of_times, sort_stats_by

            profiled_func = ProfileFunc(func, sort_stats_by)
            assert amount_of_times > 0, 'Cant profile for less then 1 run'
            for i in range(amount_of_times):
                retval, ps = profiled_func(*args, **kwargs)

            ps.print_stats()
            return retval  # returns the results of the function

        return wrapper

    # in case you don't want to specify the amount of times
    if callable(amount_of_times):
        the_func = amount_of_times  # amount_of_times is the function in here
        global DEFAULT_AMOUNT
        amount_of_times = DEFAULT_AMOUNT
        return real_decorator(the_func)
    return real_decorator
