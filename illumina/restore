from subprocess import Popen, PIPE
import tarfile, logging, threading, time, argparse

'''
This thread object converts fn to a temp file
'''
class quipjob(threading.Thread):
    def __init__(self, tfname, qfname, dest):
        threading.Thread.__init__(self)
        self.tfname=tfname
        self.qfname=qfname
        self.dest=dest
        self.status=None

    def __str__(self):
        return "Quip Job "+self.qfname

    def run(self):
        self.status=doQuip(self.tfname, self.qfname, self.dest)


'''
Generic threaded job processor
'''
def processJobs(jobs, maxthds):

    pending=jobs
    running=[]
    done=[]
    currsum=0 # running total of estimate of memory needed by all running jobs.  We don't want to exceed o.maxsum

    while pending or running:
        # start as many jobs simultaneously as allowed by maxthds and maxsum
        while pending and len(running)<maxthds:
            job=pending.pop(0)
            job.starttime=time.time()
            logger.debug("starting job %s" % (job, ))
            job.start()
            running.append(job)

        # wait for the first job to finish and handle it
        waitjob=running.pop(0)
        waitjob.join()
        if waitjob.status:
            error("Job failed, terminating")

        logger.debug("job %s finished, time %f" % (waitjob, time.time()-waitjob.starttime))
        del waitjob

QUIP="/ycga-gpfs/apps/hpc/Tools/quip/1.1.8/bin/quip"
def doQuip(tfname, qfname, dest):
    logger.debug("doing %s" % qfname)
    newfn=qfname.replace(".qp", "")
    tarcmd="tar xf %s -O %s" % (tfname, qfname)
    quipcmd="%s -d -c -o fastq" % (QUIP, )
    p1=Popen(tarcmd, shell=True, stdout=PIPE)
    p2=Popen(quipcmd, shell=True, stdin=p1.stdout, stdout=open(dest+'/'+newfn, 'w'))
    p2.wait()
    p1.wait()
    logger.debug("finished %s" % newfn)
    return p2.returncode

def doTarfile(tfname, dest):
    logger.info("Processing Tarfile %s" % tfname)
    tf=tarfile.TarFile.open(tfname)
    qpjobs=[]
    nonqpfiles=[]
    for ti in tf:
        if ti.name.endswith(".qp"):
            qpjobs.append(quipjob(tfname, ti.name, dest))
        else:
            nonqpfiles.append(ti)
    tf.extractall(path=dest, members=nonqpfiles)
    logger.info("processing %d quip files" % len(qpjobs))
    processJobs(qpjobs, o.maxthds)

''' Utility error function.  Bomb out with an error message and code '''
def error(msg):
    logger.error(msg)
    raise RuntimeError(msg)

if __name__=='__main__':

    parser=argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("-t", "--tarfile", dest="tarfile",  help="tarfile to restore")
    parser.add_argument("-f", "--files", dest="tarfiles", help="file containing tarfiles to restore")
    parser.add_argument("-d", "--destdir", dest="destdir", default=".", help="destination dir")
    parser.add_argument("-l", "--logfile", dest="logfile", default="restore", help="logfile prefix")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-n", dest="maxthds", type=int, default=20, help="max threads")

    o=parser.parse_args()
    starttime=time.time()

    # set up logger
    logger=logging.getLogger('restore')
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

    if o.tarfile and o.tarfiles:
        error("Please only specify one of -t -f")
    if not (o.tarfile or o.tarfiles):
        error("Please specify -t or -f")

    if o.tarfile:
        doTarfile(o.tarfile, o.destdir)
    elif o.tarfiles:
        for tfname in open(o.tarfiles):
            doTarfile(tfname.strip(), o.destdir)

    t = time.time() - starttime

    logger.info("Restore Finished %d sec" % t)
