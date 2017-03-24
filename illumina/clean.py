
import os, tarfile, subprocess, logging, argparse, sys, re, tempfile, time, threading, hashlib, gzip, glob, datetime, shutil


epilog="epilog"

''' utility function '''
def countTrue(*vals):
    cnt=0
    for v in vals:
        if v: cnt+=1
    return cnt

''' This function turns a negative date delta into a 6 digit date that many days in the past.  Anything else is returned unchanged '''
def fixCut(cut):
    try:
        if int(cut) < 0:
            d=datetime.datetime.now() + datetime.timedelta(days=int(cut))
            return d.strftime("%y%m%d")
    except:
        pass
    return cut

''' Utility error function.  Bomb out with an error message and code '''
def error(msg):
    logger.error(msg)
    raise RuntimeError(msg)

def getDirSize(d):
    if not o.count: return 0, 0
    total_size=0
    total_files=0
    for root, dirs, files in os.walk(d):
        total_files+=len(files)
        total_size+=sum(os.path.getsize(os.path.join(root, f)) for f in files)
        if total_files > 200: break

    return total_files, total_size

def cleanRun(rundir):
    s=stats(os.path.basename(rundir))

    if not os.path.exists("%s/Data/Intensities/BaseCalls/Unaligned" % rundir):
        # check for an "Unaligned" dir as a sanity check.  Don't do anything if it doesn't exist
        logger.warning("Not cleaning %s, no Unaligned found" % rundir)
        return s

    logger.debug("Sizing %s" % rundir)
    s.add(*getDirSize(rundir))
    logger.debug("Cleaning %s" % rundir)
    if not o.dryrun:
        try:
            shutil.rmtree("%s/Thumbnail_Images" % (rundir,))
        except OSError:
            logger.debug("Couldn't delete Thumbnail_Images")

    for d in glob.glob("%s/Data/Intensities/L00?" % (rundir,)) + glob.glob("%s/Data/Intensities/BaseCalls/L00?" % (rundir,)):
        s.add(*getDirSize(d))
        if not o.dryrun:
            try:
                shutil.rmtree(d)
            except OSError:
                logger.debug("Couldn't delete %s" % d)

    logger.info(s)
    cleaned='%s/cleaned.txt' % (rundir,)
    if not o.dryrun: open(cleaned, 'w').close()
    return s


''' utility class to hold various counts'''
class stats(object):
    def __init__(self, name):
        self.name=name
        self.files=0
        self.bytes=0
        self.start=time.time()

    def add(self, files, bytes):
        self.files+=files
        self.bytes+=bytes
    
    def comb(self, other):
        self.files+=other.files
        self.bytes+=other.bytes

    def __str__(self):
        delta=time.time()-self.start
        return "%s: %d files %d MB %f sec %.1f files/s %.1f MB/s" % (self.name, self.files, self.bytes/(1024**2), delta, float(self.files)/delta, float(self.bytes)/(1024**2)/delta)

'''
This thread object converts fn to a temp file
'''        
class cleanjob(threading.Thread):
    def __init__(self, path):
        threading.Thread.__init__(self)
        self.path=path
        self.est_sz=0
        self.true_sz=0
        self.check=[]
        self.status=None

    def __str__(self):
        return "Clean Job %s %s" % (self.name, self.path)

    def setrunner(self, runner):
        self.runner=runner

    def run(self):
        try:
            self.s=cleanRun(self.path)
            self.status=0
        except Exception as e:
            logger.error("FAILED %s %s" % (e, self.path))
        finally:
            with self.runner.joblock:
                self.runner.running.remove(self)
                self.runner.done.add(self)
                self.runner.donecnt.release()
            logger.info("%s Finished" % self)

    def finish(self, totalstats):
        if self.status==0:
            totalstats.comb(self.s)


class jobRunner(object):
    def __init__(self, jobs, maxthds, stats):
        self.pending=jobs
        self.maxthds=maxthds
        self.stats=stats

        self.running=set()
        self.done=set()
        self.finished=0

        self.memtotal=0
        self.joblock=threading.Lock()
        self.donecnt=threading.Semaphore(0)

    def run(self):
        # first, make sure none of the jobs are individually larger than o.maxsum, else we'll deadlock below
        for j in self.pending:
            if j.est_sz > o.maxsum:
                error("Found a big job %s %d" % (j.fn, j.est_sz))
        while self.pending or self.running:
            # start as many jobs simultaneously as allowed by maxthds and maxsum
            while self.pending and len(self.running)<self.maxthds and (self.memtotal+self.pending[0].est_sz) < o.maxsum:
                job=self.pending.pop(0)
                job.setrunner(self)

                self.memtotal += job.est_sz
                job.starttime=time.time()

                with self.joblock:
                    self.running.add(job)
                job.start()
                logger.debug("Starting %s", job)

            logger.debug("Pending %d Running %d Finished %d" % (len(self.pending), len(self.running), self.finished))
            # wait for the first job to finish and handle it
            self.donecnt.acquire() # wait for counter to be > 0
            waitjob=self.done.pop()
            waitjob.join()
            if waitjob.status:
                error("Job failed, terminating")
            self.memtotal -= waitjob.est_sz
            logger.debug("job %s finished, time %f, memtotal is %d" % (waitjob, time.time()-waitjob.starttime, self.memtotal))
            waitjob.finish(self.stats)
            self.finished+=1
            del waitjob
                

'''
Generic threaded job processor
'''
'''
def processJobs(jobs, maxthds, totalstats):

    pending=jobs
    running=set()
    done=set()
    currsum=0 # running total of estimate of memory needed by all running jobs.  We don't want to exceed o.maxsum
    joblock=threading.Lock()
    donesem=threading.Semaphore(0)

    # first, make sure none of the jobs are individually larger than o.maxsum, else we'll deadlock below
    for j in pending:
        if j.est_sz > o.maxsum:
            error("Found a big job %s %d" % (j.fn, j.est_sz))

    while pending or running:
        # start as many jobs simultaneously as allowed by maxthds and maxsum
        while pending and len(running)<maxthds and (currsum+pending[0].est_sz) < o.maxsum:
            job=pending.pop(0)
            job.joblock=joblock
            job.donesem=donesem

            currsum += job.est_sz
            job.starttime=time.time()
            logger.debug("starting job %s currsum is %d" % (job, currsum))
            job.start()
            with joblock:
                running.add(job)

        # wait for the first job to finish and handle it
        with donesem:
            waitjob=done.pop()
        waitjob.join()
        if waitjob.status:
            error("Job failed, terminating")

        currsum -= waitjob.est_sz
        logger.debug("job %s finished, time %f, currsum is %d" % (waitjob, time.time()-waitjob.starttime, currsum))
        waitjob.finish(totalstats)
        del waitjob
'''

if __name__=='__main__':

    parser=argparse.ArgumentParser(epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--automatic", dest="automatic", action="store_true", default=False, help="automatic settings")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually do anything")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-i", "--infile", dest="infile", help="file containing runs to clean")
    parser.add_argument("-r", "--rundir", dest="rundir", help="a single run directory to clean")
    parser.add_argument("--cutoff", dest="cutoff", default="-45", help="date cutoff; a run later than this 6 digit date will not be cleaned.  E.g. 150531.  Negative numbers are interpreted as days in the past, e.g. -45 means 45 days ago.")
    parser.add_argument("-l", "--logfile", dest="logfile", default="clean", help="logfile prefix")
    parser.add_argument("--nocount", dest="count", action="store_false", default=True, help="count files before deleting")
    parser.add_argument("--maxthds", dest="maxthds", type=int, default=20, help="max threads")
    parser.add_argument("--maxsum", dest="maxsum", type=int, default=32000000000, help="max memory to use")

    o=parser.parse_args()

    # set up logging
    logger=logging.getLogger('clean')
    formatter=logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose: hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    hf=logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
    hf.setFormatter(formatter)
    if not o.verbose: hf.setLevel(logging.DEBUG)
    logger.addHandler(hf)

    # do some validation
    # require exactly one of -r, --automatic, -i
    if countTrue(o.rundir, o.automatic, o.infile) != 1:
        error("Must specify exactly one of -r --automatic -i")
    
    if o.cutoff: o.cutoff=fixCut(o.cutoff)

    # sanity check to avoid accidents
    mindays=45
    now=int(time.strftime("%y%m%d", time.gmtime()))
    if ((now-int(o.cutoff))<mindays):
        error("--cutoff must be at least %s days in the past" % mindays)
    
    logger.debug("Invocation: " + " ".join(sys.argv))
    logger.debug("Cwd: " + os.getcwd())
    logger.debug("Options:" + str(o))

    totalstats=stats("Total")

    runs=[]

    if o.rundir:
        runs=[o.rundir]

    elif o.infile:
        fp=open(o.infile)
        for l in fp:
            # denotes comments
            if l.startswith('#'): continue
            idx=l.find('#')
            if idx!=-1:
                l=l[:idx]

            runs.append(l.strip())

    elif o.automatic:
        rds=['/ycga-ba/ba_sequencers[12356]/sequencer?/runs/[0-9]*', '/ycga-gpfs/sequencers/illumina/sequencer*/runs/[0-9]*']
        #/ycga-ba/ba_sequencers6/sequencerX/runs/160318_D00536_0228_AC8H4JANXX
        #rds=['/ycga-ba/ba_sequencers[6]/sequencer?/runs/[0-9]*']
        #rds=['/ycga-ba/ba_sequencers[12356]/sequencer?/runs/[0-9]*']
        logger.debug("Automatic pat is %s" % str(rds))
        runs=sorted(reduce(lambda a,b: a+b, [glob.glob(rd) for rd in rds]))

    # clean runs(remove L00? dirs, including bcl files)

    cleanjobs=[]
    for run in runs:
        rundate=os.path.basename(run)[0:6]
        if o.cutoff < rundate:
            logger.debug("%s too recent" % run)
            continue

        cleaned='%s/cleaned.txt' % (run,)
        if os.path.exists(cleaned):
            logger.debug("Already cleaned %s" % run)
            continue

        cleanjobs.append(cleanjob(run))

    logger.info("Found %d runs to clean" % len(cleanjobs))
    jr=jobRunner(cleanjobs, o.maxthds, totalstats)
    jr.run()

    logger.info(totalstats)
