import time

start=time.time_ns()

ns2s=1.0E9

def mysleep(seconds):
    time.sleep(seconds)

def tstamp_msg(msg, round5=False):
    passed = ( time.time_ns() - start)/ns2s
    if round5:
        passed = round(passed*2)/2
    print("%5.1f: %s" % (passed,msg) )

tstamp_msg("mytime started at %s" % time.ctime(start/ns2s))

if __name__ == "__main__":
    mysleep(0.2)
    tstamp_msg("mytime 0.2 s later")
    mysleep(0.2)
    tstamp_msg("mytime 0.2 s later, rounded to .5",round5=True)
    #print("Rounding:")
    #for i in range(20):
    #    passed = ( time.time_ns() - start)/ns2s
    #    tstamp_msg(str(round(passed,2)),round5=True)
    #    mysleep(0.1)
