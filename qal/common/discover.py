"""
Created on Oct 30, 2013

@author: Nicklas Boerjesson
"""


def discover_database_servers(_ip):
    """Uses Nmap to scan the subnet of the specified IP-address for the following RDBMS:
    SQL Server, IBM DB2, MYSQL, Oracle and Postgres."""
    return discover_services(_ip, [1433,523,1521,5432,3306], True)


def discover_services(_ip, _ports, _verbose = False):
    """Uses Nmap to scan the subnet of the specified IP-address for services on the specified ports"""
    
    try:
        import nmap                         # import nmap.py module
    except ImportError as imp:
        if str(imp) == "No module named nmap":
            raise Exception('No python Nmap wrapper, run: pip-3.x install python-nmap')                     
        else:
            raise imp

    try:
        _nm = nmap.PortScanner()      # instantiate nmap.PortScanner object
    except nmap.PortScannerError:
        raise('Nmap not found', sys.exc_info()[0])
    except Exception as e:
        print("Unexpected error:" + str(e), sys.exc_info()[0])
        raise e
    
    _ips = _ip.split(".")
    _new_ip = ".".join(_ips[0:3])
    _port_str = ",".join(str(x) for x in _ports)
    if _verbose:
        print("Scanning " +_new_ip+".2-254, ports " + _port_str)
    
    _nm.scan(hosts=_new_ip+".2-254", arguments="-p T:" + _port_str + " -T4 -sV")


    _detected_services = []
    for _host in _nm.all_hosts():
        if _nm[_host].state() == 'up':
            if _verbose:
                print('----------------------------------------------------')
                print('Host : %s (%s)' % (_host, _nm[_host].hostname()))
                print('State : %s' % _nm[_host].state())
                
            for _proto in _nm[_host].all_protocols():
                if _verbose:
                    print('----------')
                    print('Protocol : %s' % _proto)
                
                for _port in _nm[_host][_proto]:
                    if isinstance(_nm[_host][_proto][_port], dict) and _nm[_host][_proto][_port]['state'] == 'open':
                        if _verbose:
                            print(_nm[_host].hostname() + " - " +_nm[_host][_proto][_port]['product'] + _host)
                        _detected_services.append( [_host, _nm[_host].hostname(), _nm[_host][_proto][_port]['product']]) 

    return _detected_services   
                        