# -*- coding: utf-8 -*-
"""
Created on Fri Jan  4 16:49:25 2019

@author: Shenghai
"""

import os
import sys
import math
from shutil import copyfile

import pandas as pd
import numpy as np
import xlrd

from datetime import datetime

class Scrub:
    """
    I am an object
    """
    def __init__(self, dist_date=None, dictionary=None):
        self.const            = Constants()
        self.misce            = Miscellaneous()
        self.dict             = dictionary
        self.dist_date        = dist_date
        self.remit_files_path = None
        self.files            = None
        self.remit_file       = None
        self.deal_path        = None
        self.deal_key         = None
        self.stmts_path       = None
        self.ws_col           = None
        self.remit_rprt       = None
        self.mod_nm           = None
        self.ws               = None
        self.mod              = None
        self.ws_col           = None
        self.ws_ind           = None
        self.empty_row        = None
        self.row              = None
        self.stop_row         = None
        self.hist_row2        = None
        self.trans_bal        = None
        self.trans_bal_ac     = None
        self.opt_trf          = None
        self.opt_paf          = None
        self.advan_fee        = None
        self.carr_tot_remit   = None
        self.adj_df           = None
        self.agg_df           = None

        self.aggdata_path = "D:\\deals\\Carrington\\aggdata\\"
        os.makedirs(self.aggdata_path, exist_ok=True)

    def find_remit_files(self):
        """
        From January 2018 forward. There should be a yymm monthly that contains
        all carrington deals remit files 
        'D:\\deals\\carrington\\aggdata\\remit\\CMS\\yymm\\'
        """
        self.remit_files_path = "{}remit\\CMS\\{}\\" \
                                .format(self.aggdata_path, self.dist_date)
        os.makedirs(self.remit_files_path, exist_ok=True)

        # Putting all remit files in self.files when function is called
        self.files = []

        for file in os.listdir(self.remit_files_path):
            if os.path.isfile("{}\\{}".format(self.remit_files_path, file)):
                self.files.append("{}\\{}".format(self.remit_files_path, file))

    def match_file(self):
        """
        Making sure the deal matches the remit file
        Also double checking if the remit file has the correct deal name and
        Investor number.
        """
        
        if not self.files:
            print("There are no remit files for this month!!!")
            sys.exit()

        # deal_dig_name is going to be used in finding the files
        # 2016-1 or 2016-31 will be in the remit files
        if self.deal_key in self.dict.keys():
            if self.deal_key[self.const.FIVE] == '0':
                deal_dig_name = self.deal_key[self.const.ZERO:self.const.FIVE] + \
                                self.deal_key[-self.const.ONE:]
            else:
                deal_dig_name = self.deal_key[:]

            # Match the deal, deal digit name, investor else is None
            for file in self.files:
                if self.dict[self.deal_key]['invnum'] in file and deal_dig_name in file:
                    self.remit_file = file
                    break
                else:
                    self.remit_file = None

        else:
            print("Deal {} does not exist in the dictionary!!!".format(self.deal_key))
            return
        

    def init_files(self, deal_key=None):
        """
        Initialize the working file and working sheets within the object and the deal
        """
        self.deal_key = deal_key
        self.match_file()

        # Reading the excel file find the work sheet names
        # Assign working objects remit report and modification reports tabs
        self.deal_path = self.dict[self.deal_key]['deal_dir']
        ws = pd.ExcelFile(self.remit_file)
        ws_sht_nm = ws.sheet_names
        self.remit_rprt = [nm for nm in ws_sht_nm if 'remittance report' in nm.lower()]
        self.mod_nm     = [nm for nm in ws_sht_nm if 'modification' in nm.lower()]
        self.ws  = pd.read_excel(self.remit_file, sheet_name = self.remit_rprt[self.const.ZERO])
        self.mod = pd.read_excel(self.remit_file, sheet_name = self.mod_nm[self.const.ZERO])
        self.ws_col = list(self.ws.columns)

        #if KeyError comes up, then "Loan count" string is missing
        try:
            for i in np.arange(self.const.ONE, self.const.ONEHUNDREDTHOUSAND):
                if "loan count" in str(self.ws.loc[i, self.ws_col[self.const.ZERO]]).lower():
                    self.row = i
                    break
                else:
                    self.row = None

            if not self.row:
                print('Cannot find loan count string or loan count row')
                print('Check out the remit file for deal {}'.format(self.deal_key))
                print('')
                return

        except KeyError:
            print("Cannot find Loan Count string in remittance worksheet.")
            print("Going to the next deal.")
            print("")
            return

        self.empty_row = [i for i in np.arange(self.const.ZERO, int(self.row)) \
                         if math.isnan(self.ws.loc[i, self.ws_col[self.const.ZERO]])]

        self.hist_row = list(self.ws.iloc[self.row, self.const.SIX:self.const.THIRTYTHREE])

        self.stop_row = self.find_stop_row()

        self.hist_row2 = self.hist_row

        self.checking_cms_remit()

    def scrub_carr_mod(self):
        try:
            # Checking the excel files has the correct worksheet names
            # 99% of the time they have correct names
            # Sames with mod tab as well
            if self.ws.empty:
                print('Not sure which worksheet you want to look at.')
                print('You might want to check out the remit file or change the Worksheet\'s name to Remittance Report.')
                print('Going to the next deal.')
                return

            if self.mod.empty:
                print("There is No mod for {} ".format(self.deal_key))
            else:
                print("There is/are {} modification(s) for {}".format(len(self.mod), self.deal_key))
                self.create_mod()

        except:
            print("Error handling happened in scrub_carr_mod function!!!")
            print("")



    def create_mod(self):
        """
        main function to create the modification csv's
        """
        try:
            # Reset index to remit report worksheet
            # Will look up loan numbers from the modification tab to find the
            # ending balance
            self.ws_temp = self.ws.set_index(self.ws_col[self.const.ZERO])
            self.ws_temp_ind = list(self.ws_temp.index)

            balloon = []
            forbear = []
            rmg_term = []
            deal_path = []
            spec_svcg_fee = []
            last_dist = UserInput().last_mon(self.dist_date)
            next_dist = Miscellaneous().next_mon(self.dist_date)

            # Add a true bal column. True bal will be used in mod csv/dbf
            # Modify mod tab to grab the ending balance from remittance report
            self.mod['True Bal'] = pd.Series(self.const.ZERO, index=self.mod.index)

            for i in np.arange(self.const.ZERO, len(self.mod)):
                if self.mod.loc[i, self.mod.columns[self.const.ZERO]] in self.ws_temp.index[:] and \
                self.deal_key != '2017-06':
                    self.mod.loc[i, 'True Bal'] = self.ws_temp.loc[int(self.mod.loc[i, self.mod.columns[self.const.ZERO]]), \
                                                                'Ending_Balance']
                elif self.mod.loc[i, self.mod.columns[self.const.ONE]] in self.ws_temp.index[:] and \
                self.deal_key == '2017-06':
                    self.mod.loc[i, 'True Bal'] = self.ws_temp.loc[int(self.mod.loc[i, self.mod.columns[self.const.ONE]]), \
                                                                'Ending_Balance']
                else:
                    print("Cannot find the ending_balance in remittance report!!!")
                    continue

            # getting the months difference between two date within the mods tab
            def diff_month(d1, d2):
                return(d1.year - d2.year) * self.const.TWELVE + d1.month - d2.month

            for i in np.arange(self.const.ZERO, len(self.mod)):
                if pd.isnull(self.mod.loc[i, 'Post Mod Ball Pmt Date']) == False:
                    d1 = str(self.mod.loc[i, 'Post Mod Ball Pmt Date'])[self.const.ZERO:self.const.TEN]
                    d1 = datetime(int(d1[self.const.ZERO:self.const.FOUR]), \
                                  int(d1[self.const.FIVE:self.const.SEVEN]), \
                                  int(d1[self.const.EIGHT:self.const.TEN]))
                    d2 = last_dist
                    d2 = datetime(self.const.TWOTHOUSAND + int(d2[self.const.ZERO:self.const.TWO]), \
                                  int(d2[-self.const.TWO:]), self.const.ONE)
                    balloon.append(diff_month(d1, d2))
                else:
                    balloon.append(self.const.ZERO)

                dummy = np.nper(self.mod.loc[i, "Newrate"] / self.const.TWELVEHUNDRED, \
                                -self.mod.loc[i, "New PI"], self.mod.loc[i, "True Bal"], \
                                fv=self.const.ZERO)
                dummy = dummy.round(self.const.ZERO)
                rmg_term.append(dummy)
                deal_path.append(self.dict[self.deal_key]['deal_dir'])
                spec_svcg_fee.append(self.const.POINTZEROZEROSIX)

                if self.mod.loc[i, 'True Bal'] == self.const.ZERO:
                    print('Modification with $0.00 balance in deal {} loan #{}'.format(self.deal_key, \
                          self.mod.loc[i, self.mod.columns[self.const.ZERO]]))


            df = self.mod[['Loan Number', 'True Bal', 'Newrate', 'New PI']]

            for i in np.arange(self.const.ZERO, len(df)):
                if self.mod.loc[i, 'Loan Number'] in list(self.ws_temp_ind):
                    forbear.append(self.ws_temp.loc[self.mod.loc[i, 'Loan Number'], \
                                               self.ws_col[self.const.TWENTYNINE]])

            # if len(df.index) == 1 is true, SettingWithCopyWarning will pop up
            # it makes the output look cleaner
            if len(df.index) > self.const.ONE:
                df.loc[df.index, 'Forbearance Amount'] = pd.Series(forbear,  index=df.index)
                df.loc[df.index, 'Ballon Term']        = pd.Series(balloon,  index=df.index)
                df.loc[df.index, 'rmg_term']           = pd.Series(rmg_term, index=df.index)
            else:
                df.loc[:, 'Forbearance Amount'] = forbear
                df.loc[:, 'Ballon Term']        = balloon
                df.loc[:, 'rmg_term']           = rmg_term

            full_name = []
            [i for i in range(self.const.ZERO, len(self.mod)) \
             if full_name.append(self.dict[self.deal_key]['deal_full_name'])]
            mod_date = []
            [i for i in range(self.const.ZERO, len(self.mod)) \
             if mod_date.append("{}/01/20{}".format(next_dist[-self.const.TWO:], \
                                next_dist[self.const.ZERO:self.const.TWO]))]

            # if len(df.index) == 1 is true, SettingWithCopyWarning will pop up
            # it makes the output look cleaner
            if len(df.index) > self.const.ONE:
                df.loc[df.index, 'deal_path'] = pd.Series(self.dict[self.deal_key]['deal_dir'], \
                                                          index=df.index)
                df.loc[df.index, 'Mod Date'] = pd.Series(mod_date, index=df.index)
                df.loc[df.index, 'spec_svcg_fee'] = pd.Series(spec_svcg_fee, index=df.index)
            elif len(df.index) == self.const.ONE:
                df.loc[df.index, 'deal_path'] = self.dict[self.deal_key]['deal_dir']
                df.loc[df.index, 'Mod Date'] = mod_date
                df.loc[df.index, 'spec_svcg_fee'] = spec_svcg_fee

            df2 = pd.DataFrame(full_name, index=df.index)
            df = pd.concat([df2, df], axis=self.const.ONE)
            df['Newrate'] = df['Newrate'] / self.const.HUNDRED
            df = df[[self.const.ZERO, 'deal_path', 'Loan Number', 'True Bal', 'Newrate', 'spec_svcg_fee',
                     'New PI', 'Forbearance Amount', 'Ballon Term', 'rmg_term', 'Mod Date']]
            df.to_csv("d:/deals/carrington/aggdata/{}/mod_{}_{}.csv".format(self.dist_date, \
                      self.deal_key, self.dist_date), sep=",", header=False, index=False)

            print("Deal {} mods file created.".format(self.deal_key))

        except (ValueError):
            print("************************ERROR************************")
            print('Error handling happend in create_mod function in Scrub class!!!')
            print('Yo! Check Modification tab\'s date format!!!')
            print("")

        except:
            print("************************ERROR************************")
            print('Error handling happend in create_mod function in Scrub class!!!')
            print('Mod(s) csv did not create correctly')
            print("")



    def scrub_carr(self):
        """
        main function to srub remit report worksheet
        """
        #if KeyError comes up, then "Loan count" string is missing
        try:
            for i in np.arange(self.const.ONE, self.const.ONEHUNDREDTHOUSAND):
                if "loan count" in str(self.ws.loc[i, self.ws_col[self.const.ZERO]]).lower():
                    self.row = i
                    break
                else:
                    self.row = None

            if not self.row:
                print('Cannot find loan count string or loan count row')
                print('Check out the remit file for deal {}'.format(self.deal_key))
                print('')
                return

        except KeyError:
            print("Cannot find Loan Count string in remittance worksheet.")
            print("Going to the next deal.")
            print("")
            return

        try:
            # Detecting more than one empty row and put them into a list
#            self.empty_row = [i for i in np.arange(self.const.ZERO, int(self.row)) \
#                         if math.isnan(self.ws.loc[i, self.ws_col[self.const.ZERO]])]
#            self.hist_row = list(self.ws.iloc[self.row, self.const.SIX:self.const.THIRTYTHREE])
#            self.stop_row = self.find_stop_row()

            ws_col_temp = list(self.ws_col[self.const.SIX:self.const.THIRTYTHREE])
            ws_col_temp.append("tot_net_int")
            self.hist_row2.insert(self.const.ZERO, self.dict[self.deal_key]['deal_full_name'])

            self.find_misc_fees()

            self.hist_row2.append(self.advan_fee)
            self.hist_row2.append(self.carr_tot_remit)
            self.hist_row2.append(self.trans_bal)
            self.hist_row2.append(self.trans_bal_ac)

            # Not sure if below 3 lines do anything
            self.hist_row.append(self.const.ONE)
            self.hist_row.append(self.opt_trf)
            self.hist_row.append(self.opt_paf)

            self.hist_row2 = [self.hist_row2]
            os.makedirs("d:/deals/carrington/aggdata/{}".format(self.dist_date), exist_ok=True)

            df = pd.DataFrame(self.hist_row2)

            df.to_csv("d:\\deals\\carrington\\aggdata\\{}\\{}_{}.csv".format(self.dist_date, \
                      self.deal_key, self.dist_date), float_format='%.2f', sep=",", header=False, \
                      index=False)
            print("Deal {} agg file created.".format(self.deal_key))

            self.agg_df = df

        except:
            print("Error handling happened in scrub_carr function!!!")


    def find_stop_row(self):

        try:
            tot_beg_bal = float(self.const.ZERO)

            if (self.empty_row and '70000' in str(self.ws.loc[self.row, self.ws_col[self.const.ZERO]])) or \
            self.deal_key == '2017-03':
                self.stop_row = str(int(self.row))

            elif len(self.empty_row) == self.const.ONE and self.empty_row[self.const.ZERO] + \
            self.const.ONE == int(self.row) or self.deal_key == '2017-03':
                self.stop_row = self.empty_row[self.const.ZERO]

            # if there are only two empty rows, make sure empty_row[1] include all the inactive loans
            # if there are no beg bals in those inactive loans, use empty_row[1] as the stop row
            # other wise use empty_row[0]
            elif len(self.empty_row) == self.const.TWO:
                range3 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range3:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal and end_bal
                    if tot_beg_bal == self.hist_row[self.const.ZERO] and tot_end_bal == self.hist_row[self.const.ONE]:
                        return int(self.empty_row[i])

                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE) and \
                    (tot_end_bal - self.hist_row[self.const.ONE] < self.const.POINTZEROZEROONE):
                        return int(self.empty_row[i])

                    else:
                        self.stop_row = None

            elif len(self.empty_row) == self.const.THREE and \
            self.empty_row[-self.const.TWO] == int(self.row) - self.const.TWO and \
            self.empty_row[-self.const.ONE] == int(self.row) - self.const.ONE:
                # If the third empty row is second empty row are above the loan count row
                # use the second empty_row
                self.empty_row.pop()
                range5 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range5:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal
                    if tot_beg_bal == self.hist_row[self.const.ZERO]:
                        return int(self.empty_row[i])
                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE):
                        return int(self.empty_row[i])
                    else:
                        self.stop_row = None


            # two consecutive empty roself.ws in self.empty_row self.const.ZERO and self.const.ONE use emtpy_row[self.const.ZERO]
            elif len(self.empty_row) == self.const.THREE and \
            int(self.empty_row[self.const.ZERO]) == int(self.empty_row[self.const.ONE]) - self.const.ONE:
                self.empty_row.pop()
                range6 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range6:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal
                    if tot_beg_bal == self.hist_row[self.const.ZERO]:
                        return int(self.empty_row[i])
                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE):
                        return int(self.empty_row[i])
                    else:
                        self.stop_row = None

            # if last self.empty_row is right above the loan count row and last two empty roself.ws are consecutive
            elif len(self.empty_row) == self.const.THREE and \
            int(self.empty_row[-self.const.ONE]) + self.const.ONE == int(self.row) and \
            int(self.empty_row[-self.const.TWO]) + self.const.ONE == int(self.empty_row[-self.const.ONE]):
                self.empty_row.pop()
                range7 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range7:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal
                    if tot_beg_bal == self.hist_row[self.const.ZERO]:
                        return int(self.empty_row[i])
                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE):
                        return int(self.empty_row[i])
                    else:
                        self.stop_row = None

            # If it is just regular self.const.THREE blocks of loans
            elif len(self.empty_row) == self.const.THREE:
                range7 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range7:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal
                    if tot_beg_bal == self.hist_row[self.const.ZERO]:
                        return int(self.empty_row[i])
                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE):
                        return int(self.empty_row[i])
                    else:
                        self.stop_row = None

            # Two consective empty roself.ws right above the loan count row
            elif len(self.empty_row) == self.const.FOUR and \
            self.empty_row[-self.const.TWO] == int(self.row) - self.const.TWO:
                self.empty_row.pop()
                range5 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range5:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal including the inactive loans
                    # Double check if the decimal does not match for some reason
                    if tot_beg_bal == self.hist_row[self.const.ZERO] and tot_end_bal == self.hist_row[self.const.ONE]:
                        return int(self.empty_row[i])
                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE) and \
                    (tot_end_bal - self.hist_row[self.const.ONE] < self.const.ZERO.self.const.ZEROself.const.ONE):
                        return int(self.empty_row[i])
                    else:
                        self.stop_row = None

            # two consecutive empty roself.ws in self.empty_row self.const.ZERO and self.const.ONE use emtpy_row[self.const.ZERO]
            elif len(self.empty_row) == self.const.FOUR and \
            int(self.empty_row[self.const.ZERO]) == int(self.empty_row[self.const.ONE]) - self.const.ONE:
                range6 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range6:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal
                    if tot_beg_bal == self.hist_row[self.const.ZERO]: return int(self.empty_row[i])
                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE):
                        return int(self.empty_row[i])
                    else:
                        self.stop_row = None

            elif len(self.empty_row) == self.const.FOUR and \
            int(self.empty_row[-self.const.ONE]) + self.const.ONE == int(self.row):
                range7 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range7:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal
                    if tot_beg_bal == self.hist_row[self.const.ZERO] and tot_end_bal == self.hist_row[self.const.ONE]:
                        return int(self.empty_row[i])
                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE):
                        return int(self.empty_row[i])
                    else:
                        self.stop_row = None

            elif len(self.empty_row) == self.const.FOUR and int(self.empty_row[-self.const.ONE]) == int(self.empty_row[-self.const.TWO]) + self.const.ONE:
                range8 = np.arange(len(self.empty_row)-self.const.ONE, -self.const.ONE, -self.const.ONE)
                for i in range8:
                    tot_beg_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SIX]].sum(skipna=True)
                    tot_end_bal = self.ws.loc[:self.empty_row[i], self.ws_col[self.const.SEVEN]].sum(skipna=True)

                    # Check if all the loans add up to beg_bal
                    if tot_beg_bal == self.hist_row[self.const.ZERO]: return int(self.empty_row[i])
                    elif (tot_beg_bal - self.hist_row[self.const.ZERO] < self.const.POINTZEROZEROONE):
                        return int(self.empty_row[i])
                    else:
                        self.stop_row = None

            else:
                print('Check out {} {} remit file!'.format(self.deal_key, self.dist_date))
                print('You might want to delete some extra empty rows.')
                return

            # check again with regular payment column and kick out warning message if there is inconsistency
            regular_pmt_amt = self.ws.loc[:self.stop_row, self.ws_col[self.const.EIGHT]].sum(skipna=True)

            # Exception for SMLT201703
            if regular_pmt_amt == self.hist_row[self.const.TWO] or self.deal_key == "2017-03":
                pass
            else:
                print("***************WARNING***************")
                print("Dat file might have included uneccessary loans!!!")
                print("Check out the remit file and/or dat file for deal {}!!".format(self.deal_key))
                print("***************WARNING***************")
                print("")

            return self.stop_row

        except(TypeError):
            raise
        except:
            print("Error handling happened in find_stop_row function!!!")


    def checking_cms_remit(self):
        """
        Doing additional numbers checking within the scrub function
        """
        try:
            def_prin_w_off    = float(self.const.ZERO)
            prin_w_off        = float(self.const.ZERO)
            def_trans_bal     = float(self.const.ZERO)
            self.trans_bal    = float(self.const.ZERO)
            self.trans_bal_ac = float(self.const.ZERO)

            for num in np.arange(self.const.ZERO, int(self.stop_row)):
                # if fmv column is not empty, add up the new Q & R columns
                if self.ws.loc[num, self.ws_col[self.const.THIRTYTWO]] > self.const.ZERO:
                    self.trans_bal += self.ws.loc[num, self.ws_col[self.const.SIX]]
                    def_trans_bal += self.ws.loc[num, self.ws_col[self.const.TWENTYEIGHT]]

                # if def prin w/off not empty and fmv > 0, zero out the cell
                if not math.isnan(self.ws.loc[num, self.ws_col[self.const.SIXTEEN]]) and \
                self.ws.loc[num, self.ws_col[self.const.THIRTYTWO]] > self.const.ZERO:
                    self.ws.loc[num, self.ws_col[self.const.SIXTEEN]] = float(self.const.ZERO)

                # if prin w/off not empty and fmv > 0, zero out the cell
                if not math.isnan(self.ws.loc[num, self.ws_col[self.const.SEVENTEEN]]) and \
                self.ws.loc[num, self.ws_col[self.const.THIRTYTWO]] > self.const.ZERO:
                    self.ws.loc[num, self.ws_col[self.const.SEVENTEEN]] = float(self.const.ZERO)

            # summing up the new q and r columns, deferred prin wirte-off and prin write-off
            # A lot of the clean-up is being done here. To get the new writeoffs and etc.
            def_prin_w_off = self.ws.loc[:self.stop_row, self.ws_col[self.const.SIXTEEN]].sum(skipna=True)
            prin_w_off = self.ws.loc[:self.stop_row, self.ws_col[self.const.SEVENTEEN]].sum(skipna=True)
            self.trans_bal_ac = self.trans_bal + def_trans_bal

            # Create net calc int for each loan
            # Repalce total_upb cell with total net interest
            total_net_int = self.const.ZERO
            for num in np.arange(self.const.ZERO, int(self.stop_row)):
                if self.ws.loc[num, self.ws_col[self.const.SIX]] > self.const.ZERO:
                    total_net_int += self.ws.loc[num, self.ws_col[self.const.SIX]] * \
                                     self.ws.loc[num, self.ws_col[self.const.NINE]] / \
                                     float(self.const.TWELVEHUNDRED) - \
                                     self.ws.loc[num, self.ws_col[self.const.ELEVEN]]

            #Do a loan by loan balance check.  Writes a message with any loans that have an error.
            # Must be done BEFORE adjustments to FMV loans.
            # Beg_bal + beg_def_prin - prin_payment - prin_writeoff - end_def_prin - end_bal - fmv
            for num in np.arange(self.const.ZERO, int(self.stop_row)):
                bal_check = self.ws.loc[num, self.ws_col[self.const.SIX]] + \
                            self.ws.loc[num, self.ws_col[self.const.TWENTYEIGHT]] - \
                            self.ws.loc[num, self.ws_col[self.const.THIRTEEN]] - \
                            self.ws.loc[num, self.ws_col[self.const.SEVENTEEN]] - \
                            self.ws.loc[num, self.ws_col[self.const.TWENTYNINE]] - \
                            self.ws.loc[num, self.ws_col[self.const.SEVEN]] - \
                            self.ws.loc[num, self.ws_col[self.const.THIRTYTWO]]

                if abs(bal_check) > self.const.POINTZEROFIVE:
                    print('Loan level balance check fail for Loan # {0} by {1} check it out!' \
                          .format(self.ws.loc[num, self.ws_col[self.const.ZERO]], bal_check))
                    input('Press Enter to continue >>>')
                else:
                    pass

            self.hist_row2 = self.hist_row
            self.hist_row2[self.const.TEN] = def_prin_w_off
            self.hist_row2[self.const.ELEVEN] = prin_w_off
            self.hist_row2.append(total_net_int)

            # Check hist_row if they are consistent with the sum of all loans' numbers
            beg_def_prin = self.ws.loc[:self.stop_row, self.ws_col[self.const.TWENTYEIGHT]].sum(skipna=True)
            end_def_prin = self.ws.loc[:self.stop_row, self.ws_col[self.const.TWENTYNINE]].sum(skipna=True)

            if beg_def_prin - self.hist_row2[self.const.TWENTYTWO] > self.const.POINTZEROFIVE:
                print('There is inconsistency between hist_row Beg_Def_Prin and remit_file Beg_Def_Prin amount')
                print('Script is going to change the number in csv')
                print('Watch out for other inconsitency in the remit file')
                print("")
                self.hist_row2[self.const.TWENTYTWO] = beg_def_prin

            if end_def_prin - self.hist_row2[self.const.TWENTYTHREE] > self.const.POINTZEROFIVE:
                print('There is inconsistency between hist_row End_Def_Prin and Loan Total End_Def_Prin amount')
                print('Script is going to change the number in csv.')
                print('Watch out for other inconsitency in the remit file!')
                print("")
                self.hist_row2[self.const.TWENTYTHREE] = end_def_prin

        except:
            print("Error handling happened in checking_cms_remit function!!!")


    def create_dat(self):
        """
        Creating 7-column dat file
        """
        try:
            # Create the dat file in the deal remit folder
            # 7-columns carrington dat
            # carrington #, beg_bal, end_bal, prin, gross int, prin w/r/loss, end def prin
            # Search for other dat and/or inp file to create deal name and date extention
            c_file = os.listdir(self.dict[self.deal_key]['deal_dir'])
            for file in c_file:
                if '.inp' in file:
                    five_lttr_deal_name = str(file[self.const.ZERO:self.const.FIVE])
                    break
                else:
                    five_lttr_deal_name = None

            if Miscellaneous().is_empty(five_lttr_deal_name) == True:
                print("Cannot find deal name!!!")
                print('')
                return

            if Miscellaneous().is_empty(five_lttr_deal_name) == False:
                dat = open('{}{}_{}.dat'.format(self.dict[self.deal_key]['deal_dir'], \
                                                five_lttr_deal_name, self.dist_date), 'w+')
            else:
                dat = open('{}_{}.dat'.format(self.dict[self.deal_key]['deal_dir'], \
                                              self.dist_date), 'w+')

                print('Make sure you rename the dat file')

            for loan in np.arange(self.const.ZERO, int(self.stop_row)):
                # (1) Carrington Ln#
                if not math.isnan(self.ws.loc[loan, self.ws_col[self.const.ZERO]]):
                    dat.write("{} ".format(str(self.ws.loc[loan, self.ws_col[self.const.ZERO]])))
                else:
                    pass

                # (2) beg_bal, if not empty write beg bal
                if not math.isnan(self.ws.loc[loan, self.ws_col[self.const.SIX]]):
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.SIX]]))
                else:
                    dat.write("0.00 ")

                # (3) end bal, if not empty write end bal
                if not math.isnan(self.ws.loc[loan, self.ws_col[self.const.SEVEN]]):
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.SEVEN]]))
                else:
                    dat.write("0.00 ")

                # (4) prin payment, if not empty write n column
                #                   else if not empty in prin payment and there is a fmv, write prin_pmt + fmv
                #                   else if empty in prin_pmt and there is a fmv, write fmv
                #                   else write zero
                if not math.isnan(self.ws.loc[loan, self.ws_col[self.const.THIRTEEN]]) and \
                self.ws.loc[loan, self.ws_col[self.const.THIRTEEN]] != self.const.ZERO and \
                self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]] > self.const.ZERO:
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.THIRTEEN]] + \
                                                self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]]))
                elif self.ws.loc[loan, self.ws_col[self.const.THIRTEEN]] == self.const.ZERO and \
                self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]] > self.const.ZERO:
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]]))
                elif not math.isnan(self.ws.loc[loan, self.ws_col[self.const.THIRTEEN]]):
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.THIRTEEN]]))
                else:
                    dat.write("0.00 ")

                # (5) int, if not empty write interest
                if not math.isnan(self.ws.loc[loan, self.ws_col[self.const.TEN]]):
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.TEN]]))
                else:
                    dat.write("0.00 ")

                # (6) prin w/r/loss, if not empty write prin_w_off
                #                    else if beg_def_bal not empty and fmv not empty, beg_bal - fmv + beg_def_bal
                #                    else if
                if self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]] == float(self.const.ZERO) and \
                self.ws.loc[loan, self.ws_col[self.const.SEVENTEEN]] == float(self.const.ZERO) and \
                self.ws.loc[loan, self.ws_col[self.const.SIXTEEN]] == float(self.const.ZERO):
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.SEVENTEEN]]))
                elif not math.isnan(self.ws.loc[loan, self.ws_col[self.const.TWENTYEIGHT]]) and \
                self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]] > self.const.ZERO:
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.SIX]] - \
                                                self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]] + \
                                                self.ws.loc[loan, self.ws_col[self.const.TWENTYEIGHT]]))
                elif not math.isnan(self.ws.loc[loan, self.ws_col[self.const.SEVENTEEN]]) and \
                self.ws.loc[loan, self.ws_col[self.const.SEVENTEEN]] != self.const.ZERO:
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.SEVENTEEN]]))
                elif self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]] > self.const.ZERO:
                    dat.write('{0:.2f} '.format(self.ws.loc[loan, self.ws_col[self.const.SIX]] - \
                                                self.ws.loc[loan, self.ws_col[self.const.THIRTYTWO]]))
                else:
                    dat.write("0.00 ")

                # (7) end def prin
                if not math.isnan(self.ws.loc[loan, self.ws_col[self.const.TWENTYNINE]]):
                    dat.write('{0:.2f}\n'.format(self.ws.loc[loan, self.ws_col[self.const.TWENTYNINE]]))
                else:
                    dat.write("0.00\n")

            dat.close()

            # Copy the dat file into remictax/aggdata/cmsdats
            # That's where ETICMS interface will read in the collateral dats
            copyfile('{}{}_{}.dat'.format(self.dict[self.deal_key]['deal_dir'], five_lttr_deal_name, self.dist_date), \
                     "d:/deals/remictax/aggdata/cmsdats/{}_{}.dat".format(five_lttr_deal_name, self.dist_date))

            copyfile('{}{}_{}.dat'.format(self.dict[self.deal_key]['deal_dir'], five_lttr_deal_name, self.dist_date), \
                     "{}remit/{}_{}.dat".format(self.dict[self.deal_key]['deal_dir'], five_lttr_deal_name, self.dist_date))

            print("Deal {} dat file created.".format(self.deal_key))

        except:
            print("Error handling happened in create_dat function!!!")


    def find_misc_fees(self):
        """
        Find the given carrington fees besides carrington remittance
        """
        try:
            for i in np.arange(self.const.ONE, self.const.ONEHUNDREDTHOUSAND):

                if "hamp funds" in str(self.ws.loc[i, "Next_Due_Date"]).lower():
                    fee_row = i + self.const.ONE
                    break
                else:
                    fee_row = None

            for i in np.arange(self.const.ONE, self.const.ONEHUNDREDTHOUSAND):
                if "remittance total" in str(self.ws.loc[i, "Next_Due_Date"]).lower() or \
                "total remittance" in str(self.ws.loc[i, "Next_Due_Date"]).lower():
                    remit_stop_row = i
                    break
                else:
                    remit_stop_row = None

            if not fee_row:
                print("Cannot find fees!!!")
                print("Going to the next deal.")
                print("")
                return

            if not remit_stop_row:
                print("Cannot find total remittance amount!!!")
                print("Going to the next deal.")
                return

            self.advan_fee = self.const.ZERO

            for i in np.arange(fee_row, remit_stop_row):
                if "remittance total" not in str(self.ws.loc[i, "Next_Due_Date"]) or \
                "total remittance" not in str(self.ws.loc[i, "Next_Due_Date"]):
                    if math.isnan(self.ws.loc[i, "Beginning_Balance"]):
                        continue
                    else:
                        self.advan_fee += self.ws.loc[i, "Beginning_Balance"]

            self.opt_trf = self.ws.loc[remit_stop_row, 'Ending_Balance']
            self.opt_paf = self.ws.loc[remit_stop_row, 'Current_Regular_Pmt_Amt']

            if pd.isna(self.opt_trf) or pd.isna(self.opt_paf):
                print("Cannot find optimal trustee/paying agent fee in the remit file!" + \
                      " for deal {}".format(self.deal_key))
                print("")

            if self.ws.loc[remit_stop_row, "Beginning_Balance"] > float(self.const.ZERO):
                self.carr_tot_remit = self.ws.loc[remit_stop_row, "Beginning_Balance"]
            elif self.ws.loc[remit_stop_row, "Beginning_Balance"] < float(self.const.ZERO):
                self.carr_tot_remit = self.ws.loc[remit_stop_row, "Beginning_Balance"]
                self.advan_fee = -self.carr_tot_remit + self.advan_fee
                self.carr_tot_remit = float(self.const.ZERO)
            elif self.ws.loc[remit_stop_row, "Beginning_Balance"] == float(self.const.ZERO):
                self.carr_tot_remit = float(self.const.ZERO)
            elif self.ws.loc[remit_stop_row, "Beginning_Balance"] < float(self.const.ZERO) and self.deal_key == "2014-05":
                self.carr_tot_remit = self.ws.loc[remit_stop_row, "Beginning_Balance"]
                self.advan_fee += -self.ws.loc[remit_stop_row, "Beginning_Balance"]
                self.carr_tot_remit = float(self.const.ZERO)


        except:
            print("Error handling happened in find_misc_fees function!!!")


    def create_adj(self):
        """
        find the statement downloaded from wells fargo website and find the actual
        servicing fee and other fees that will be used to process deals.
        """
        try:
            c_file = os.listdir(self.dict[self.deal_key]['deal_dir'])
            for file in c_file:
                if '.inp' in file:
                    five_lttr_deal_name = str(file[self.const.ZERO:self.const.FIVE])
                    break
                else:
                    five_lttr_deal_name = None

            # Grabbing fees and blocker cash for po deals only in the stmts + dist_date path
            stmts_path = "{}stmts\\{}\\R20{}\\".format(self.aggdata_path, self.deal_key, self.dist_date)

            # List the files and fine the right file with distribution/fee numbers
            wells_files = os.listdir(stmts_path)
            if len(wells_files) == self.const.FOUR:
                for file in wells_files:
                    if str(five_lttr_deal_name[-self.const.ONE:]) + "_RMT" in file and ".xls" in file:
                        fee_non_rt = "{}{}".format(stmts_path, file)
                    if "RT_RMT" in file and ".xls" in file:
                        rt_rmt = "{}{}".format(stmts_path, file)

            elif len(wells_files) > self.const.FOUR:
                print("There are more than 4 files in the directory, you might want to check it out!!!")
            else:
                print("Something is funky about the wells files, you might want to check it out!!!")

            fee_wb = xlrd.open_workbook(fee_non_rt, formatting_info=True)
            fee_ws = fee_wb.sheet_by_index(self.const.ZERO)
            blocker_cash = float(self.const.ZERO)

            # Find paying agent fee, trustee fee, blocker cash. For some deals we need Trigger
            # amount, trigger adjustment amount, and trigger threshold
            for i in np.arange(self.const.HUNDREDTWENTY, self.const.HUNDREDFIFTY):
                if "class a" in str(fee_ws.cell(i, self.const.SEVEN)).lower() and \
                "paying agent fee" in str(fee_ws.cell(i, self.const.SEVEN)).lower():
                    paf = fee_ws.cell_value(i, self.const.ELEVEN)
                    break
                else:
                    paf = None

            for i in np.arange(self.const.HUNDREDTWENTY, self.const.HUNDREDFIFTY):
                if "class a" in str(fee_ws.cell(i, self.const.SEVEN)).lower() and \
                "trustee fee" in str(fee_ws.cell(i, self.const.SEVEN)).lower():
                    trt_fee = fee_ws.cell_value(i, self.const.ELEVEN)
                    break
                else:
                    trt_fee = None

            for i in np.arange(self.const.HUNDREDTWENTY, self.const.HUNDREDFIFTYFIVE):
                if "class a service fee" in str(fee_ws.cell(i, self.const.SEVEN)).lower() or \
                "class a servicing fee" in str(fee_ws.cell(i, self.const.SEVEN)).lower() or \
                "servicing fee a" in str(fee_ws.cell(i,self.const.SEVEN)).lower():
                    actual_svcg_fee = fee_ws.cell_value(i, self.const.ELEVEN)
                    break
                else: actual_svcg_fee = None

            for i in np.arange(self.const.TWOHUNDREDTWENTY, self.const.TWOHUNDREDSEVENTY):
                if ("class b" in str(fee_ws.cell_value(i, self.const.TWO)).lower() and \
                "available funds" in str(fee_ws.cell_value(i, self.const.TWO)).lower()) or \
                ("class b" in str(fee_ws.cell_value(i, self.const.TWO)).lower() and \
                "available distribution amount" in str(fee_ws.cell_value(i, self.const.TWO)).lower()):
                    blocker_cash += float((fee_ws.cell_value(i, self.const.SEVEN)).replace(',', ''))
                    break
                else:
                    blocker_cash = float(self.const.ZERO)

            for i in np.arange(self.const.TWOHUNDREDTWENTY, self.const.TWOHUNDREDSEVENTY):
                if ("class b" in str(fee_ws.cell_value(i, self.const.TWO)).lower() and \
                "success service fee paid" in str(fee_ws.cell_value(i, self.const.TWO)).lower()) or \
                ("class b" in str(fee_ws.cell_value(i, self.const.TWO)).lower() and \
                "success servicing fee paid" in str(fee_ws.cell_value(i, self.const.TWO)).lower()):
                    suc_svcg_paid = float((fee_ws.cell_value(i, self.const.SEVEN)).replace(',', ''))
                    blocker_cash += suc_svcg_paid
                    break
                else:
                    suc_svcg_paid = None

            for i in np.arange(self.const.TWOHUNDREDTHIRTYFIVE, self.const.TWOHUNDREDFIFTYFIVE):
                if "periodic trigger amount" in str(fee_ws.cell_value(i, self.const.TWO)).lower():
                    prd_trig_amt = float((fee_ws.cell_value(i, self.const.SEVEN)).replace(',', ''))
                    break
                else:
                    prd_trig_amt = None

            for i in np.arange(self.const.TWOHUNDREDTHIRTYFIVE, self.const.TWOHUNDREDFIFTYFIVE):
                if "trigger adjustment amount" in str(fee_ws.cell_value(i, self.const.TWO)).lower() or \
                ("trigger threshold" in str(fee_ws.cell_value(i, self.const.TWO)).lower() and \
                 "adjustment" in str(fee_ws.cell_value(i, self.const.TWO)).lower() and \
                 "amount" in str(fee_ws.cell_value(i, self.const.TWO)).lower()):
                    trig_adj_amt = float((fee_ws.cell_value(i, self.const.SEVEN)).replace(',', ''))
                    break
                else:
                    trig_adj_amt = None

            for i in np.arange(self.const.TWOHUNDREDTHIRTYFIVE, self.const.TWOHUNDREDFIFTYFIVE):
                if str(fee_ws.cell_value(i, self.const.TWO)) == "Trigger Threshold":
                    trig_thr= float((fee_ws.cell_value(i, self.const.SEVEN)).replace(',', ''))
                    break
                else:
                    trig_thr = None

            rt_wb = xlrd.open_workbook(rt_rmt, formatting_info=True)
            rt_ws = rt_wb.sheet_by_index(self.const.ZERO)

            for row in np.arange(self.const.FIFTEEN, self.const.TWENTYFIVE):
                if 'A' in str(rt_ws.cell_value(row, self.const.ZERO)):
                    class_a_row = row
                    break
                else:
                    class_a_row = None

            for row in np.arange(self.const.FIFTEEN, self.const.TWENTYFIVE):
                if 'PO' in str(rt_ws.cell_value(row, self.const.ZERO)):
                    class_po_row = row
                    break
                else:
                    class_po_row = None

            if self.misce.is_empty(class_po_row) == True:
                for row in np.arange(class_a_row - self.const.THREE, self.const.TWENTY):
                    for col in np.arange(self.const.SIX, self.const.TEN):
                        if col == self.const.NINE:
                            class_a_int_dist = rt_ws.cell_value(class_a_row, col - self.const.THREE)
                            class_a_prin_dist = rt_ws.cell_value(class_a_row, col - self.const.TWO)
                            end_cert_bal_a = rt_ws.cell_value(class_a_row, col)
                            tot_dist = rt_ws.cell_value(class_a_row, col + self.const.ONE)
                            break
                        else:
                            class_a_int_dist = None
                            class_a_prin_dist = None
                            end_cert_bal_a = None
                            tot_dist = None

                    if end_cert_bal_a >= self.const.ZERO and tot_dist >= self.const.ZERO:
                        break

                end_cert_bal_po = None
                tot_dist_po = None
                class_po_int_dist = None
                class_po_prin_dist = None
                deal_type = 'A'

            else:
                for row in np.arange(class_a_row - self.const.THREE, self.const.EIGHTEEN):
                    for col in np.arange(self.const.SIX, self.const.TEN):
                        if "Ending" in str(rt_ws.cell_value(row, col)):
                            end_cert_bal_a     = rt_ws.cell_value(class_a_row,  col)
                            end_cert_bal_po    = rt_ws.cell_value(class_po_row, col)
                            class_a_int_dist   = rt_ws.cell_value(class_a_row,  col - self.const.THREE)
                            class_a_prin_dist  = rt_ws.cell_value(class_a_row,  col - self.const.TWO)
                            class_po_int_dist  = rt_ws.cell_value(class_po_row, col - self.const.THREE)
                            class_po_prin_dist = rt_ws.cell_value(class_po_row, col - self.const.TWO)
                            tot_dist_po        = rt_ws.cell_value(class_po_row, col + self.const.ONE)
                            tot_dist           = rt_ws.cell_value(class_a_row,  col + self.const.ONE)
                            break
                        else:
                            end_cert_bal_a     = None
                            end_cert_bal_po    = None
                            class_a_int_dist   = None
                            class_a_prin_dist  = None
                            class_po_int_dist  = None
                            class_po_prin_dist = None
                            tot_dist           = None

                    if end_cert_bal_a >= self.const.ZERO and \
                    end_cert_bal_po >= self.const.ZERO and \
                    tot_dist >= self.const.ZERO and tot_dist_po >= self.const.ZERO:
                        break

                deal_type = 'PO'

            data = [[
                    self.dict[self.deal_key]["deal_full_name"],
                    end_cert_bal_a,
                    end_cert_bal_po, # if any
                    class_a_int_dist,
                    class_a_prin_dist,
                    tot_dist,
                    class_po_int_dist,
                    class_po_prin_dist,
                    tot_dist_po,
                    paf + trt_fee,
                    actual_svcg_fee,
                    blocker_cash,
                    prd_trig_amt,
                    trig_adj_amt,
                    trig_thr,
                    None,
                    deal_type,
                    self.const.ONE, # Default only to 1 group of loans
                    "{}/15/20{}".format(self.dist_date[-self.const.TWO:], \
                                        self.dist_date[self.const.ZERO:self.const.TWO])
                    ]]

            df = pd.DataFrame(data)

            df.to_csv("d:/deals/carrington/aggdata/{}/adj_{}_{}.csv".format(self.dist_date, \
                      self.deal_key, self.dist_date), sep=",", header=False, index=False)
            print("Deal {} adj file created.".format(self.deal_key))

            self.adj_df = df

            if self.agg_df is not None and self.adj_df is not None:
                self.cms_cash_check()
            else:
                pass

        except:
            print("Error handling happened in creat_adj function!!!")


    def cms_cash_check(self):
        try:
            # first round of cash checks before ETICMS application
            # Some deals are set up different
            beg_bal      = self.agg_df.iloc[self.const.ZERO, self.const.ONE]
            end_bal      = self.agg_df.iloc[self.const.ZERO, self.const.TWO]
            sched_prin   = self.agg_df.iloc[self.const.ZERO, self.const.EIGHT]
            unsched_prin = self.agg_df.iloc[self.const.ZERO, self.const.TWENTYSEVEN]
            gross_int    = self.agg_df.iloc[self.const.ZERO, self.const.FIVE]      + \
                           self.agg_df.iloc[self.const.ZERO, self.const.FIFTEEN]   + \
                           self.agg_df.iloc[self.const.ZERO, self.const.SIXTEEN]   + \
                           self.agg_df.iloc[self.const.ZERO, self.const.SEVENTEEN] + \
                           self.agg_df.iloc[self.const.ZERO, self.const.EIGHTEEN]  + \
                           self.agg_df.iloc[self.const.ZERO, self.const.TWENTYSIX] + \
                           self.agg_df.iloc[self.const.ZERO, self.const.TWENTYNINE]

            servicing          = self.adj_df.iloc[self.const.ZERO, self.const.TEN]
            prin_writeoff      = self.agg_df.iloc[self.const.ZERO, self.const.TWELVE]
            trans_bal_2        = self.agg_df.iloc[self.const.ZERO, self.const.THIRTYTWO]
            fmv                = self.agg_df.iloc[self.const.ZERO, self.const.TWENTYSEVEN]
            beg_def_bal        = self.agg_df.iloc[self.const.ZERO, self.const.TWENTYTHREE]
            end_def_bal        = self.agg_df.iloc[self.const.ZERO, self.const.TWENTYFOUR]
            carr_remit         = self.agg_df.iloc[self.const.ZERO, self.const.THIRTY]
            paf_trustee        = self.adj_df.iloc[self.const.ZERO, self.const.NINE]
            class_a_tot_remit  = self.adj_df.iloc[self.const.ZERO, self.const.FIVE]
            class_po_tot_remit = self.adj_df.iloc[self.const.ZERO, self.const.EIGHT]

            gross_cash = gross_int + sched_prin + unsched_prin

            # balance, principal, loss, fmv check, should be all the same acrross all carrington deals
            check_1 = end_bal - (beg_bal - sched_prin - unsched_prin - prin_writeoff - trans_bal_2 + \
                        fmv + beg_def_bal - end_def_bal)

            if abs(check_1) > self.const.POINTZEROFIVE:
                print("Check 1 is off by {:2f}".format(abs(check_1)))

            if class_po_tot_remit == None and "remit_check_1" in self.dict[self.deal_key]['check_type']:
                remit_check_1 = carr_remit - servicing - paf_trustee - class_a_tot_remit
                if abs(remit_check_1) > self.const.POINTZEROFIVE:
                    print("Remit check 1 is off by {:2f}".format(abs(remit_check_1)))

            elif "remit_check_1" in self.dict[self.deal_key]['check_type']:
                remit_check_1 = carr_remit - servicing - paf_trustee - class_a_tot_remit - class_po_tot_remit
                if abs(remit_check_1) > self.const.POINTZEROFIVE:
                    print("Remit check 1 is off by {:2f}".format(abs(remit_check_1)))

            elif "remit_check_2" in self.dict[self.deal_key]['check_type']:
                remit_check_2 = gross_cash - fmv - carr_remit
                if abs(remit_check_2) > self.const.POINTZEROFIVE:
                    print("Remit check 2 is off by {:2f}".format(abs(remit_check_2)))

            elif class_po_tot_remit == None and "remit_check_3" in self.dict[self.deal_key]['check_type']:
                remit_check_3 = gross_cash - fmv - paf_trustee - servicing - class_a_tot_remit
                if abs(remit_check_3) > self.const.POINTZEROFIVE:
                    print("Remit check 3 is off by {:2f}".format(abs(remit_check_3)))

            elif "remit_check_3" in self.dict[self.deal_key]['check_type']:
                remit_check_3 = gross_cash - fmv - paf_trustee - servicing - class_a_tot_remit - class_po_tot_remit
                if abs(remit_check_3) > self.const.POINTZEROFIVE:
                    print("Remit check 3 is off by {:2f}".format(abs(remit_check_3)))

            elif class_po_tot_remit == None and "remit_check_4" in self.dict[self.deal_key]['check_type']:
                remit_check_4 = gross_cash - fmv - carr_remit
                if abs(remit_check_4) > self.const.POINTZEROFIVE:
                    print("Remit check 4 is off by {:2f}".format(abs(remit_check_4)))

            elif "remit_check_4" in self.dict[self.deal_key]['check_type']:
                remit_check_4 = gross_cash - fmv - carr_remit
                if abs(remit_check_4) > self.const.POINTZEROFIVE:
                    print("Remit check 4 is off by {:2f}".format(abs(remit_check_4)))

            else:
                print("You're shit out of luck!!!")

        except:
            print("Error handling happened in cms_cash_check function!!!")


class UserInput:

    def __init__(self):
        self.const = Constants()

    def user_input_yymm(self):
        # Asking user for year and month input as yymm
        # Retun the date as a 4-digit string as yymm
        try:
            while True:

                distribution_yymm = input("Please enter the distrituion month and year, " + \
                                               "(example 1701 for January 2017): ")
                try:
                    if (int(distribution_yymm[self.const.TWO:self.const.FOUR]) >= self.const.ONE) and \
                    (int(distribution_yymm[self.const.TWO:self.const.FOUR]) <= self.const.TWELVE) and \
                    (len(distribution_yymm) == self.const.FOUR):

                        month = int(distribution_yymm[self.const.TWO:self.const.FOUR])
                        year = int(distribution_yymm[self.const.ZERO:self.const.TWO])
                        if month < self.const.TEN:
                            month = "0{}".format(str(month))
                        else:
                            month = str(month)

                        distribution_yymm= "{}{}".format(str(year), str(month))
                        break

                    else:
                        print("Let's try this again!")
                        continue

                except:
                    print("Let's try this again!")
                    pass

            # return dist_date as a string like 1801 for Jan. 2018
            return distribution_yymm

        except:
            print("Error handling happened in user_input function!!!")
            print("")


    def last_mon(self, dist_date=None):

        if int(dist_date[-self.const.TWO:]) == self.const.ONE:
            last_mon = self.const.TWELVE
            cur_yr = int(dist_date[self.const.ZERO:self.const.TWO])
            last_yr = cur_yr - self.const.ONE
            last_mon = "{}{}".format(str(last_yr), str(last_mon))
        else:
            if (int(dist_date[-self.const.TWO:]) - self.const.ONE) < self.const.TEN:
                cur_mon = int(dist_date[-self.const.TWO:])
                last_mon = cur_mon - self.const.ONE
                cur_yr = int(dist_date[self.const.ZERO:self.const.TWO])
                last_mon = "{}0{}".format(str(cur_yr), str(last_mon))
            else:
                cur_mon = int(dist_date[-self.const.TWO:])
                last_mon = cur_mon - self.const.ONE
                cur_yr = int(dist_date[self.const.ZERO:self.const.TWO])
                last_mon = "{}{}".format(str(cur_yr), str(last_mon))

        return last_mon




class Miscellaneous:

    def __init__(self):
        self = self

    def is_empty(self, any_structure):
        # empty structure is False by default
        if any_structure:
            return False
        else:
            return True

    def next_mon(self, dist_date):
        '''
        Take yymm as an input and return yymm as a string
        '''
        if int(dist_date[-2:]) == 12:
            next_mon = '01'
            cur_yr = int(dist_date[0:2])
            next_yr = cur_yr + 1
            next_mon = str(next_yr) + next_mon
        else:
            if (int(dist_date[-2:]) + 1) < 10:
                cur_mon = int(dist_date[-2:])
                next_mon = cur_mon + 1
                cur_yr = int(dist_date[0:2])
                next_mon = str(cur_yr) + "0" + str(next_mon)
            else:
                cur_mon = int(dist_date[-2:])
                next_mon = cur_mon + 1
                cur_yr = int(dist_date[0:2])
                next_mon = str(cur_yr) + str(next_mon)

        return next_mon


class Constants:
    """
    Frequently used constants
    """
    def __init__(self):
        self.ZERO = 0
        self.ONE  = 1
        self.TWO  = 2
        self.THREE = 3
        self.FOUR = 4
        self.FIVE = 5
        self.SIX  = 6
        self.SEVEN = 7
        self.EIGHT = 8
        self.NINE = 9
        self.TEN  = 10
        self.ELEVEN = 11
        self.TWELVE = 12
        self.THIRTEEN = 13
        self.FIFTEEN  = 15
        self.SIXTEEN = 16
        self.SEVENTEEN = 17
        self.EIGHTEEN = 18
        self.TWENTY   = 20
        self.TWENTYTWO = 22
        self.TWENTYTHREE = 23
        self.TWENTYFOUR  = 24
        self.TWENTYFIVE  = 25
        self.TWENTYSIX   = 26
        self.TWENTYSEVEN = 27
        self.TWENTYEIGHT = 28
        self.TWENTYNINE = 29
        self.THIRTY = 30
        self.THIRTYTWO = 32
        self.THIRTYTHREE = 33
        self.HUNDRED = 100
        self.HUNDREDTWENTY = 120
        self.HUNDREDFIFTY  = 150
        self.HUNDREDFIFTYFIVE = 155
        self.TWOHUNDREDTWENTY = 220
        self.TWOHUNDREDTHIRTYFIVE = 235
        self.TWOHUNDREDFIFTY  = 250
        self.TWOHUNDREDFIFTYFIVE = 255
        self.TWOHUNDREDSEVENTY = 270
        self.TWOHUNDREDSEVENTYFIVE = 275
        self.TWELVEHUNDRED = 1200
        self.TWOTHOUSAND = 2000
        self.POINTZEROFIVE = 0.05
        self.POINTZEROZEROONE = 0.001
        self.POINTZEROZEROSIX = 0.006
        self.ONEHUNDREDTHOUSAND = 100000
