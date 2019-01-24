# -*- coding: utf-8 -*-
"""
Created on Fri Jan  4 16:29:02 2019

@author: Shenghai
"""

import sys
import threading

from miscellaneous_cms_v4 import UserInput
from miscellaneous_cms_v4 import Scrub


# This the deal selection list class
# Usually will be on very top of the script
class MyDeals:
    def __init__(self):
        self.dict = {
                
################### My deals            
'2014-03': {'deal_dir': 'D:/deals/Carrington/SMAC/2014-03/', 'deal_full_name': 'SMLT1403', 'deal_type': 'A' , 'check_type': 'remit_check_3', 'invnum': '2181'},
'2014-05': {'deal_dir': 'D:/deals/Carrington/SMAC/2014-05/', 'deal_full_name': 'SMLT1405', 'deal_type': 'A' , 'check_type': 'remit_check_3', 'invnum': '2191'},
'2015-08': {'deal_dir': 'D:/deals/Carrington/SMAC/2015-08/', 'deal_full_name': 'SMLT1508', 'deal_type': 'PO', 'check_type': 'remit_check_1', 'invnum': '2209'},
'2015-09': {'deal_dir': 'D:/deals/Carrington/SMAC/2015-09/', 'deal_full_name': 'SMLT1509', 'deal_type': 'PO', 'check_type': 'remit_check_1', 'invnum': '2212'},
'2016-01': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-01/', 'deal_full_name': 'SMLT1601', 'deal_type': 'PO', 'check_type': 'remit_check_4', 'invnum': '2215'},
'2016-04': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-04/', 'deal_full_name': 'SMLT1604', 'deal_type': 'PO', 'check_type': 'remit_check_1', 'invnum': '2224'},
'2016-21': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-21/', 'deal_full_name': 'SMLT1621', 'deal_type': 'A' , 'check_type': 'remit_check_4', 'invnum': '2218'},
'2016-23': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-23/', 'deal_full_name': 'SMLT1623', 'deal_type': 'A' , 'check_type': 'remit_check_3', 'invnum': '2227'},
'2016-31': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-31/', 'deal_full_name': 'SMLT1631', 'deal_type': 'A' , 'check_type': 'remit_check_1', 'invnum': '2237'},
'2016-32': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-32/', 'deal_full_name': 'SMLT1632', 'deal_type': 'A' , 'check_type': 'remit_check_1', 'invnum': '2248'},
'2018-36': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-36/', 'deal_full_name': 'UMAC1836', 'deal_type': 'A' , 'check_type': 'remit_check_1', 'invnum': '2324'},
##
######## Brian's deals
#'2013-20': {'deal_dir': 'D:/deals/Carrington/SMAC/2013-20/', 'deal_full_name': 'SMLT1320', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2154'},
#'2013-21': {'deal_dir': 'D:/deals/Carrington/SMAC/2013-21/', 'deal_full_name': 'SMLT1321', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2156'},
#'2014-01': {'deal_dir': 'D:/deals/Carrington/SMAC/2014-01/', 'deal_full_name': 'SMLT1401', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2176'},
#'2014-04': {'deal_dir': 'D:/deals/Carrington/SMAC/2014-04/', 'deal_full_name': 'SMLT1404', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2184'},
#'2015-03': {'deal_dir': 'D:/deals/Carrington/SMAC/2015-03/', 'deal_full_name': 'SMLT1503', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2196'},
#'2015-04': {'deal_dir': 'D:/deals/Carrington/SMAC/2015-04/', 'deal_full_name': 'SMLT1504', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2200'},
#'2015-05': {'deal_dir': 'D:/deals/Carrington/SMAC/2015-05/', 'deal_full_name': 'SMLT1505', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2203'},
#'2015-07': {'deal_dir': 'D:/deals/Carrington/SMAC/2015-07/', 'deal_full_name': 'SMLT1507', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2206'},
#'2016-05': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-05/', 'deal_full_name': 'SMLT1605', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2221'},
#'2016-06': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-06/', 'deal_full_name': 'SMLT1606', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2229'},
#'2016-07': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-07/', 'deal_full_name': 'SMLT1607', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2231'},
#'2016-08': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-08/', 'deal_full_name': 'SMLT1608', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2233'},
#'2016-09': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-09/', 'deal_full_name': 'SMLT1609', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2239'},
#'2016-10': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-10/', 'deal_full_name': 'SMLT1610', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2242'},
#'2016-11': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-11/', 'deal_full_name': 'SMLT1611', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2245'},
#'2016-12': {'deal_dir': 'D:/deals/Carrington/SMAC/2016-12/', 'deal_full_name': 'SMLT1612', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2250'},
#'2017-01': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-01/', 'deal_full_name': 'SMLT1701', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2257'},
#'2017-02': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-02/', 'deal_full_name': 'SMLT1702', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2261'},
#'2017-03': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-03/', 'deal_full_name': 'SMLT1703', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2268'},
#'2017-04': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-04/', 'deal_full_name': 'SMLT1704', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2270'},
#'2017-05': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-05/', 'deal_full_name': 'SMLT1705', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2284'},
#'2017-06': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-06/', 'deal_full_name': 'SMLT1706', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2286'},
#'2017-09': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-09/', 'deal_full_name': 'SMLT1709', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2295'},
#'2017-21': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-21/', 'deal_full_name': 'SMLT1721', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2253'},
#'2017-31': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-31/', 'deal_full_name': 'UMLT1731', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2259'},
#'2017-32': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-32/', 'deal_full_name': 'UMLT1732', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2263'},
#'2017-33': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-33/', 'deal_full_name': 'UMLT1733', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2265'},
#'2017-34': {'deal_dir': 'D:/deals/Carrington/SMAC/2017-34/', 'deal_full_name': 'UMLT1734', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2272'},
#'2018-01': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-01/', 'deal_full_name': 'SMLT1801', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2298'},
#'2018-02': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-02/', 'deal_full_name': 'SMLT1802', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2318'},
#'2018-03': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-03/', 'deal_full_name': 'SMLT1803', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2315'},
#'2018-04': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-04/', 'deal_full_name': 'SMLT1804', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2321'},
#'2018-05': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-05/', 'deal_full_name': 'SMLT1805', 'deal_type': 'PO', 'check_type': 'remit_check_2', 'invnum': '2329'},
#'2018-31': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-31/', 'deal_full_name': 'UMAC1831', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2304'},
#'2018-32': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-32/', 'deal_full_name': 'UMAC1832', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2307'},
#'2018-33': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-33/', 'deal_full_name': 'UMAC1833', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2301'},
#'2018-34': {'deal_dir': 'D:/deals/Carrington/SMAC/2018-34/', 'deal_full_name': 'UMAC1834', 'deal_type': 'A' , 'check_type': 'remit_check_2', 'invnum': '2310'},

############### Terminated deals
#'2015-01': {'deal_dir': 'D:/deals/Carrington/SMAC/2015-01/', 'deal_full_name': 'SMLT1501', 'deal_type': 'A' , 'check_type': 'remit_check_3', 'invnum': '2194'},
}


if __name__ == '__main__':
    dist_date = UserInput().user_input_yymm()
    print("")
    scrub = Scrub(dist_date=dist_date, dictionary=MyDeals().dict)
    scrub.find_remit_files()
    for deal in MyDeals().dict.keys():
        scrub.init_files(deal_key=deal)
        threading.Thread(target=scrub.scrub_carr()).start()
        threading.Thread(target=scrub.scrub_carr_mod()).start()
        threading.Thread(target=scrub.create_dat()).start()
        threading.Thread(target=scrub.create_adj()).start()
        print("")
           
    print("Press Enter to exit -->>>")
    input()
    sys.exit()