from ray.util import ActorPool


class ActorPoolExtended(ActorPool):
    def submit(self, key, method, *args, **kwargs):
        """Some improvements over the native ``submit`` method found in
        ray source code:

        https://github.com/ray-project/ray/blob/master/python/ray/util/actor_pool.py#L95:

        1. extend signature from (fn, value) to (key, method, *args, **kwargs)
        2. get rid of additional layer of function (``fn``).
        """
        if self._idle_actors:
            actor = self._idle_actors.pop()
            future = actor.run.remote(key, method, *args, **kwargs)
            future_key = tuple(future) if isinstance(future, list) else future
            self._future_to_actor[future_key] = (self._next_task_index, actor)
            self._index_to_future[self._next_task_index] = future
            self._next_task_index += 1
        else:
            self._pending_submits.append((key, method, args, kwargs))

    def _return_actor(self, actor):
        """Modified from the original codes found in ray source code:

        https://github.com/ray-project/ray/blob/master/python/ray/util/actor_pool.py#L218

        to accomodate the changes made to the ``submit`` function.
        """
        self._idle_actors.append(actor)
        if self._pending_submits:
            key, method, args, kwargs = self._pending_submits.pop(0)
            self.submit(key, method, *args, **kwargs)
