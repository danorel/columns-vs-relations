from ..server import app


def runJob(MRJobClass, args_arr, runner='hadoop'):
    args_arr.extend(['-r', runner])
    app.logger.info("Starting %s job on %s" % (MRJobClass.__name__, type))
    job = MRJobClass(args=args_arr)
    runner = job.make_runner()
    runner.run()
    app.logger.info("Finished %s job" % MRJobClass.__name__)
    return job, runner

