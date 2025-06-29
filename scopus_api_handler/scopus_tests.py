import requests


key = "YOUR KEY GOES HERE"
def makeReq(req, doi, key):
    # original request did not contain abstracts
   # params = f"&query={doi}&apiKey={key}&field=author,affiliation,prism:doi"
    params = f"&query={doi}&apiKey={key}&facets%20=%20authname(count=20);af-id(count=50);country(count=50)&view=COMPLETE"
    response = requests.get(req+params)
    return response
# Print the response
# &facets%20=%20authname(count=20);af-id(count=50);country(count=50)&view=COMPLETE

def rowConversion(rows):
    return "+OR+".join([f"DOI({row[0]})" for row in rows])

def reqHandler(dois):
    req = f"https://api.elsevier.com/content/search/scopus?httpaccept=application/json"
    doiString = rowConversion(dois)

    resp = makeReq(req, doiString, key)
    print(f'REMAINING REQUESTS:{resp.headers.get("X-RateLimit-Remaining")}')
    try:
        if resp.headers.get("status_code") ==429 or int(resp.headers.get("X-RateLimit-Remaining")) ==0:
            return {"status":"ABORT", "payload":resp.json() }

        return {"status":"OK", "payload":resp.json() }

    except:
        print("failed request")
        return {"status":"OK", "payload":[{"REQUEST":"FAIL"}] }

