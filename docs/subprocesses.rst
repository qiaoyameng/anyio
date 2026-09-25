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

Chaining processes into pipelines
---------------------------------

To connect several processes so that each one feeds its standard output to the next
one (like the ``|`` operator in a shell, but without involving a shell), use
:func:`~open_process_pipeline`::

    from anyio import open_process_pipeline, run


    async def main():
        commands = [["gunzip", "data.txt.gz"], ["grep", "needle"]]
        async with open_process_pipeline(commands) as pipeline:
            async for chunk in pipeline.stdout:
                print(chunk.decode(), end="")

    run(main)

The processes are connected directly with operating system pipes, so intermediate
data streams from one process to the next without passing through the Python
process. The standard error output of each process is captured separately and
drained in the background, so a process flooding its stderr cannot deadlock the
pipeline.

The :class:`~ProcessPipeline` object yielded by the context manager gives you access
to the individual processes along with the standard input of the first process and
the standard output of the last one. When the context manager exits, all the
processes are waited for, and if any of them exited with a non-zero exit code and
``check`` is true (the default), :exc:`~ProcessPipelineError` is raised, containing
the exit codes and captured stderr output of every stage. If a process fails to
start, or the enclosed block raises an exception or is cancelled, all remaining
processes are killed and reaped.

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
