def runJob(MRJobClass, argsArr, loc='local'):
    if loc == 'emr':
        argsArr.extend(['-r', 'emr'])
    print("Starting %s job on %s" % (MRJobClass.__name__, loc))
    mrJob = MRJobClass(args=argsArr)
    runner = mrJob.make_runner()
    runner.run()
    print("Finished %s job" % MRJobClass.__name__)
    return mrJob, runner

