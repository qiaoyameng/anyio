Using subprocesses
==================

.. py:currentmodule:: anyio

AnyIO allows you to run arbitrary executables in subprocesses, either as a one-shot call
or by opening a process handle for you that gives you more control over the subprocess.

You can either give the command as a string, in which case it is passed to your default
shell (equivalent to ``shell=True`` in :func:`subprocess.run`), or as a sequence of
strings (``shell=False``) in which case the executable is the first item in the sequence
and the rest are arguments passed to it.

Running one-shot commands
-------------------------

To run an external command with one call, use :func:`~run_process`::

    from anyio import run_process, run


    async def main():
        result = await run_process('ps')
        print(result.stdout.decode())

    run(main)

The snippet above runs the ``ps`` command within a shell. To run it directly::

    from anyio import run_process, run


    async def main():
        result = await run_process(['ps'])
        print(result.stdout.decode())

    run(main)

Working with processes
----------------------

When you have more complex requirements for your interaction with subprocesses, you can
launch one with :func:`~open_process`::

    from anyio import open_process, run
    from anyio.streams.text import TextReceiveStream


    async def main():
        async with await open_process(['ps']) as process:
            async for text in TextReceiveStream(process.stdout):
                print(text)

    run(main)

See the API documentation of :class:`~.abc.Process` for more information.

.. _RunInProcess:

Running functions in worker processes
-------------------------------------

When you need to run CPU intensive code, worker processes are better than threads
because, with the exception of the experimental free-threaded builds of Python 3.13 and
later, current implementations of Python cannot run Python code in multiple threads at
once.

Exceptions to this rule are:

#. Blocking I/O operations
#. C extension code that explicitly releases the Global Interpreter Lock
#. :doc:`Subinterpreter workers <subinterpreters>`
   (experimental; available on Python 3.13 and later)

If the code you wish to run does not belong in this category, it's best to use worker
processes instead in order to take advantage of multiple CPU cores.
This is done by using :func:`.to_process.run_sync`::

    import time

    from anyio import run, to_process


    def cpu_intensive_function(arg1, arg2):
        time.sleep(1)
        return arg1 + arg2

    async def main():
        result = await to_process.run_sync(cpu_intensive_function, 'Hello, ', 'world!')
        print(result)

    # This check is important when the application uses to_process.run_sync()
    if __name__ == '__main__':
        run(main)

Running one-time setup in worker processes
******************************************

If every worker process needs to do some expensive one-time setup before it can
serve requests (such as loading a large dictionary or model into memory), you
can pass an ``initializer`` callable along with ``initargs``, similar to
:class:`multiprocessing.Pool`. The initializer is run once in each newly
spawned worker process, before that worker handles its first request::

    from anyio import run, to_process


    def load_dictionary(path):
        global dictionary
        with open(path) as file:
            dictionary = set(file.read().split())


    def count_known_words(words):
        return sum(word in dictionary for word in words)


    async def main():
        result = await to_process.run_sync(
            count_known_words, ['hello', 'world'],
            initializer=load_dictionary, initargs=('/usr/share/dict/words',),
        )
        print(result)

    if __name__ == '__main__':
        run(main)

Worker processes are pooled per initializer configuration: two calls only share
worker processes if the initializer and every element of ``initargs`` are
identical (compared by object identity, not equality). Calls made with
different initializers or initializer arguments therefore never reuse each
other's worker processes. If the initializer raises an exception, it is
propagated to the caller and the failed worker process is discarded.

Technical details
*****************

There are some limitations regarding the arguments and return values passed:

* the arguments must be pickleable (using the highest available protocol)
* the return value must be pickleable (using the highest available protocol)
* the target callable must be importable (lambdas and inner functions won't work)

Other considerations:

* Even ``cancellable=False`` runs can be cancelled before the request has been sent to
  the worker process
* If a cancellable call is cancelled during execution on the worker process, the worker
  process will be killed
* The worker process imports the parent's ``__main__`` module, so guarding for any
  import time side effects using ``if __name__ == '__main__':`` is required to avoid
  infinite recursion
* ``sys.stdin`` and ``sys.stdout``, ``sys.stderr`` are redirected to ``/dev/null`` so
  :func:`print` and :func:`input` won't work
* Worker processes terminate after 5 minutes of inactivity, or when the event loop is
  finished

  * On asyncio, either :func:`asyncio.run` or :func:`anyio.run` must be used for proper
    cleanup to happen
* Multiprocessing-style synchronization primitives are currently not available
