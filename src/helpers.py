import re
import socket
import ipaddress
import time
import os


### Try to resolve IP addresses that were filled for host names and exclude data not relevant to the project
### If it's a host name, verify it's part of the configuration
###   If it's an IP - try to resolve 
###     If it cannot be resolved - serach in ps_meta for the host name
def ResolveHost(es, host):
    is_host = re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", host)
    h = ''
    u = []

    try:
        # There are data that is collected in ElasticSearch, but it's not relevant to the SAND project.
        # It is neccessary to check if it is part of the configuration by searching in ps_meta index.
        if is_host:
            is_valid = {
                          "size": 1,
                          "_source": ["host"],
                          "query": {
                              "term" : {
                              "host.keyword" : {
                                "value" : host
                              }
                            }
                          }
                        }
            res = es.search("ps_meta", body=is_valid)
            if res['hits']['hits']:
                h = host
            else:
                u.append(host)
                u.append("Host not part of configuration")
        else:
            # Sometimes host name is not resolved and instead IP address is added. Try to resolve the given IP
            h = socket.gethostbyaddr(host)[0]
    except Exception as inst:
#         v = {'external_address.ipv6_address'{'value':host}}
        version = ipaddress.ip_address(host).version 
        if (version == 4):
            v = {'external_address.ipv6_address':{'value':host}}
        elif (version == 6):
            v = {'external_address.ipv6_address':{'value':host}}

        # It is possible that the IP got changed but the host is still valid. Check if the ip exists in ps_meta and take the host name. 
        check_hostname = {
                  "_source": ["host"],
                  "size": 1, 
                    "query": {
                      "term" : v
                    }
                }
        
#         print(check_hostname)
        res = es.search("ps_meta", body=check_hostname)
        if res['hits']['hits']:
            h = res['hits']['hits'][0]['_source']['host']
#             print('IP',host, 'was found in ps_meta:', h)
        # if it's a unknown hostname the ip check will fail
        u.append(host)
        u.append(inst.args)

    return {'resolved': h, 'unknown': u}