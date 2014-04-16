"""
Created on Oct 30, 2013

@author: Nicklas Boerjesson
"""
import platform

import sys
from qal.common.strings import empty_if_none

def platform_to_int():
    """Returns an integer depending on platform."""
    if platform.system().lower() == "linux":
        return 0
    elif platform.system().lower() == "win32":
        return 1
    elif platform.system().lower() == "darwin":
        return 2
    else:
        return 3

def import_error_to_help(_module, _err_obj, _pip_package, _apt_package, _win_package, _import_comment=None):
    """Usable to create a helpful error message if a module is missing"""
    if str(_err_obj) == "No module named '" + _module + "'":
        _err_msg = "The python " + get_python_versions(_style="Minor") + " module \"" + _module + "\" is not installed.\n"
        _err_msg += ["Run pip-" + get_python_versions(_style="Minor") + " install " + _pip_package +
                     empty_if_none(" or sudo apt-get install " + _apt_package, _apt_package),
                     "Run pip install " + _pip_package + empty_if_none(" or download " + _win_package +
                                                                       " and install from source.", _win_package),
                     "If available on your platform, run pip" + get_python_versions(_style="Minor") + " install " + _pip_package +
                     " otherwise download and install from source."][platform_to_int()]
        return _err_msg + empty_if_none("\n" + str(_import_comment), _import_comment)
    else:
        return str(_err_obj)


def discover_database_servers(_ip):
    """Uses Nmap to scan the subnet of the specified IP-address for the following RDBMS:
    SQL Server, IBM DB2, MYSQL, Oracle and Postgres."""
    return discover_services(_ip, [1433,523,1521,5432,3306], True)


def discover_services(_ip, _ports, _verbose = False):
    """Uses Nmap to scan the subnet of the specified IP-address for services on the specified ports"""
    
    try:
        import nmap                         # import nmap.py module
    except ImportError as _err:
        raise Exception(import_error_to_help(_module= "nmap", _err_obj = _err, _pip_package = "python-nmap", _apt_package = None, _win_package = None))

    try:
        _nm = nmap.PortScanner()      # instantiate nmap.PortScanner object
    except nmap.PortScannerError:
        raise Exception('Nmap not found(nmap not installed on the system?)', sys.exc_info()[0])
    except Exception as e:
        print("Unexpected error:" + str(e), sys.exc_info()[0])
        raise Exception(str(e))
    
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

def get_python_versions(_style=None):
    _major, _minor, _release, _state, _build = sys.version_info

    if _style == "Major":
        return str(_major)
    elif _style == "Minor":
        return str(_major) + "." + str(_minor)
    elif _style == "Release":
        return str(_major) + "." + str(_minor) + "." + str(_release)
    elif _style == "Full" or _style is None:
        return str(_major) + "." + str(_minor) + "." + str(_release) + " " + _state + " build " + str(_build)
    else:
        raise Exception("Error in get_python_versions: Invalid _style-parameter :'"+ _style + "'")


