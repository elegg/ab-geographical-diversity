from db_wrap_abehav import wrappedDB, wrappedAbstractDB
from scopus_worker import AffiliationRequester, extractEntries
from scopus_tests import reqHandler

# general info requester
#affRequest = AffiliationRequester(wrappedDB, reqHandler, extractEntries )

#affRequest.start()

# abstract requester
affRequest = AffiliationRequester(wrappedAbstractDB, reqHandler, extractEntries )
affRequest.start()
