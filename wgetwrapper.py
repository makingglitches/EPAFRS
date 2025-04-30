import subprocess
import time

# https://www.gnu.org/software/wget/manual/html_node/Exit-Status.html


def fetch_html_with_wget(url,dltimeout=5, tries = 2, jobtimeout=10)->dict:

    started = time.time()

    res = {}

    try:        
        result = subprocess.run(
            ["wget",
            '-T',
            str(dltimeout), 
            '-t', 
            str(tries), 
            '-U', 
            'Mozilla/5.0 (X11; U; Linux i686 (x86_64); en-GB; rv:1.9.0.1) Gecko/2008070206 Firefox/3.0.1', 
            "-qO-", 
            url],  # -q = quiet, -O- = output to stdout
            capture_output=True,
            text=True,
            timeout=jobtimeout
        )
        # gpt suggested adding a timeout, which I previously did
        # thing is that when the call succeeds it doesn't take 3 seconds
        # but with only two jobs runniong a lot of lag occurs.
        
        res =  {'stdout':result.stdout, 'stderr':result.stderr, 'returncode':result.returncode}
    except subprocess.TimeoutExpired:
        res =  {'stdout':'', 'stderr':'Process Timeout', 'returncode': 255}

    ended = time.time()
    
    res['totaltime'] = ended - started
    
    return res


if __name__=="__main__":
    goog = fetch_html_with_wget('https://www.google.com')
    print(goog)
