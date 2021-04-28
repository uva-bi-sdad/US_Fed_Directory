# US_Fed_Directory

This repository uses the OpenGSA [SAM.Gov Federal Hierarchy Public API](https://open.gsa.gov/api/fh-public-api/#user-account-api-key-creation) to build a directory of U.S. Federal entities.

This repository assumes the code is being executed in [Rivanna](https://www.rc.virginia.edu/service/high-performance-computing/) and the user has access to the `sdad@postgis1`.

The environmental variables that the user should have in their environment are:
```
PGHOST = postgis1
DB_USR = { computing_id }
DB_PWD = { postgis1_password }
SAM_KEY = { SAM_API_KEY }
```

The SAM_KEY 

The SAM APIs have the following Rate Limits:
- Non-Government Users - 10 requests/day
- Non-Government Users associated to an Entity - 1,000 requests/day
- Government Users - 1,000 requests/day
- Government Users with a System Account - No Limit
