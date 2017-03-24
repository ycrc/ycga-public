
import glob, itertools, os, logging, argparse, time

def fltr(r):
    rn=os.path.basename(r)
    try:
        dt=int(rn[:6])
        return dt < o.cutoff
    except:
        print ("weird %s" % r)
        return False

runlocs=['/ycga-ba/ba_sequencers?/sequencer?/runs/*',
'/ycga-gpfs/sequencers/panfs/sequencers*/sequencer?/runs/*',
'/ycga-gpfs/sequencers/illumina/sequencer?/runs/*']

equiv=(
    ("/ycga-gpfs/sequencers/panfs", "/SAY/archive/YCGA-729009-YCGA/archive/panfs"),
    ("/ycga-ba", "/SAY/archive/YCGA-729009-YCGA/archive/ycga-ba"),
    ("/ycga-gpfs/sequencers/illumina", "/SAY/archive/YCGA-729009-YCGA/archive/ycga-gpfs/sequencers/illumina")
)

def chkArchive(r):
    for o, a in equiv:
        if r.startswith(o):
            arun=r.replace(o,a)
            chkfile=arun+'/'+os.path.basename(r)+"_finished.txt"
            st=os.path.exists(chkfile)
            logger.debug("check %s %s" % (chkfile, st))
            return st


if __name__=='__main__':

    parser=argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("-c", "--cutoff", dest="cutoff", type=int, required=True, help="cutoff date YYMMDD")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually delete")
    parser.add_argument("-l", "--logfile", dest="logfile", default="delete", help="logfile prefix")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")

    o=parser.parse_args()
    starttime=time.time()

    # set up logger
    logger = logging.getLogger('delete')
    formatter = logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc = logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose:
        hc.setLevel(logging.INFO)
        logger.addHandler(hc)

    hf = logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
    hf.setFormatter(formatter)
    if not o.verbose:
        hf.setLevel(logging.DEBUG)
        logger.addHandler(hf)

    runs=itertools.chain.from_iterable([glob.glob(loc) for loc in runlocs])

    oldruns=[r for r in runs if fltr(r)]

    for r in oldruns:
        if not chkArchive(r):
            logger.error("No archive for %s" % r)
        else:
            logger.debug("Delete %s" % (r,))
            if not o.dryrun:
                logger.debug("Do delete")



