import subprocess

# https://www.gnu.org/software/wget/manual/html_node/Exit-Status.html


def fetch_html_with_wget(url,timeout=5)->dict:
    result = subprocess.run(
        ["wget",
         '-T',
         str(timeout), 
         '-t', 
         '2', 
         '-U', 
         'Mozilla/5.0 (X11; U; Linux i686 (x86_64); en-GB; rv:1.9.0.1) Gecko/2008070206 Firefox/3.0.1', 
         "-qO-", 
         url],  # -q = quiet, -O- = output to stdout
        capture_output=True,
        text=True
    )
    
    return {'stdout':result.stdout, 'stderr':result.stderr, 'returncode':result.returncode}


if __name__=="__main__":
    goog = fetch_html_with_wget('https://www.google.com')
    print(goog)
