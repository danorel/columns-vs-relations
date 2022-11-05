from ..server import app


def runJob(MRJobClass, argsArr, loc='local'):
    if loc == 'emr':
        argsArr.extend(['-r', 'emr'])
    app.logger.info("Starting %s job on %s" % (MRJobClass.__name__, loc))
    mrJob = MRJobClass(args=argsArr)
    runner = mrJob.make_runner()
    runner.run()
    app.logger.info("Finished %s job" % MRJobClass.__name__)
    return mrJob, runner

