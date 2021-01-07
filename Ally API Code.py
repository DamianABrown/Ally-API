# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 14:34:28 2021

@author: damia
"""

import requests
from requests_oauthlib import OAuth1
import json
import mysql.connector
from mysql.connector import Error

class AllyAPI:    
    
    def __init__(self):
        self.url = 'https://devapi.invest.ally.com/v1/'
        self.authorize = OAuth1('PRIVATE KEY INFORMATION',
                                'PRIVATE KEY INFORMATION',
                                'PRIVATE KEY INFORMATION',
                                'PRIVATE KEY INFORMATION')
        self.parameters = {'symbols': '''mmm, abt, abbv, abmd, acn, atvi, adbe, 
                           amd, aap, aes, afl, a, apd, akam, alk, alb, are, 
                           alxn, algn, alle, lnt, all, googl, goog, mo, amzn, 
                           amcr, aee, aal, aep, axp, aig, amt, awk, amp, abc, 
                           ame, amgn, aph, adi, anss, antm, aon, apa, aiv, 
                           aapl, amat, aptv, adm, anet, ajg, aiz, t, ato, adsk, 
                           adp, azo, avb, avy, bkr, bll, bac, bk, bax, bdx, brk.b, 
                           bby, bio, biib, blk, ba, bkng, bwa, bxp, bsx, bmy, 
                           avgo, br, bf.b, chrw, cog, cdns, cpb, cof, cah, kmx, 
                           ccl, carr, ctlt, cat, cboe, cbre, cdw, ce, cnc, cnp, 
                           cern, cf, schw, chtr, cvx, cmg, cb, chd, ci, cinf, 
                           ctas, csco, c, cfg, ctxs, clx, cme, cms, ko, ctsh, 
                           cl, cmcsa, cma, cag, cxo, cop, ed, stz, coo, cprt, 
                           glq, ctva, cost, cci, csx, cmi, cvs, dhi, dhr, dri, 
                           dva, de, dal, xray, dvn, dxcm, fang, dlr, dfs, disca, 
                           disck, dish, dg, dltr, d, dpz, dov, dow, dte, duk, 
                           dre, dd, dxc, emn, etn, ebay, ecl, eix, ew, ea, emr, 
                           etr, eog, efx, eqix, eqr, ess, el, etsy, evrg, es, 
                           re, exc, expe, expd, exr, xom, ffiv, fb, fast, frt,
                           fdx, fis, fitb, fe, frc, fisv, flt, flir, fls, fmc, 
                           f, ftnt, ftv, fbhs, foxa, fox, ben, fcx, gps, grmn, 
                           it, gd, ge, gis, gm, gpc, gild, gl, gpn, gs, gww, 
                           hal, hbi, hig, has, hca, peak, hsic, hsy, hes, hpw, 
                           hlt, hfc, holx, hd, hon, hrl, hst, hwm, hpq, hum, 
                           hban, hii, iex, idxx, info, itw, ilmn, incy, ir, 
                           intc, ice, ibm, ip, ipg, iff, intu, isrg, ivz, ipgp, 
                           iqv, irm, jkhy, j, jbht, sjm, jnj, jci, jpm, jnpr, 
                           ksu, k, key, keys, kmb, kim, kmi, klac, khc, kr, lb, 
                           lhx, lh, lrcx, lw, lvs, leg, ldos, len, lly, lnc, 
                           lin, lyv, lkq, lmt, l, low, lumn, lyb, mtb, mro, 
                           mpc, mktx, mar, mmc, mlm, mas, ma, mkc, mxim, mcd, 
                           mck, mdt, mgm, mchp, mu, msft, maa, mhk, tap, mdlz, 
                           mnst, mco, ms, mos, msi, msci, ndaq, nov, ntap, nflz, 
                           nwl, nem, nwsa, nws, nee, nlsn, nke, ni, nsc, ntrs, 
                           noc, nlok, nclh, nrg, nue, nvda, nvr, orly, oxy, 
                           odfl, omc, oke, orcl, otis, pcar, pkg, ph, payx, 
                           payc, pypl, pnr, pbct, pep, pki, prgo, pfe, pm, psx, 
                           pnw, pxd, pnc, pool, ppg, ppl, pfg, pg, pgr, pld, 
                           pru, peg, psa, phm, pvh, qrvo, pwr, qcom, dgx, rl, 
                           rjf, rtx, o, reg, regn, rf, rsg, rmd, rhi, rok, rol, 
                           rop, rost, rcl, spgi, crm, sbac, slb, stx, see, sre, 
                           now, shw, spg, swks, slg, sna, so, luv, swk, sbux, 
                           stt, ste, syk, sivb, syf, snps, syy, tmus, trow, 
                           ttwo, tpr, tgt, tel, fti, tdy, tfx, ter, tsla, txn, txt, 
                           tmo, tif, tjx, tsco, tt, tdg, trv, tfc, twtr, tyl, 
                           tsn, udr, ulta, usb, uaa, ua, unp, ual, unh, ups, 
                           uri, uhs, unm, vlo, var, vtr, vrsn, vrsk, vz, vrtx, 
                           vfc, viac, vtrs, v, vnt, vno, vmc, wrb, wab, wmt, 
                           wba, dis, wn, wat, wec, wfc, well, wst, wdc, wu, 
                           wrk, wy, whr, wmb, wltw, wynn, xel, xrx, xlnx, xyl, 
                           yum, zbra, zbh, zion, zts'''}
        self.options_parameters = {'symbol': 'spx',
                                   'query': '20210115'}
    
    def marketStatus(self):
        market_url_ext = 'market/clock.json'
        market_url = self.url + market_url_ext
        response = requests.get(market_url)
        content = response.content
        info = json.loads(content)
        market_status = list(info['response']['status'].values())[0]
        return market_status
    
    def getQuotes(self):
        quote_url_ext = 'market/ext/quotes.json'
        quote_url = self.url + quote_url_ext
        response = requests.get(quote_url, auth = self.authorize, 
                                params = self.parameters)
        content = response.content
        info = json.loads(content)
        quote_dict = info['response']['quotes']['quote']
        return quote_dict
    
    def getOptions(self):
        options_url_ext = 'market/options/search.json'
        options_url = self.url + options_url_ext
        response = requests.get(options_url, auth = self.authorize, 
                                params = self.options_parameters)
        content = response.content
        info = json.loads(content)
        options_dict = info['response']['quotes']['quote']
        print(options_dict)
        return options_dict
    
class SQL:
    
    def __init__(self):
        self.host_name = 'localhost'
        self.user_name = 'root'
        self.password =  'PRIVATE PASSWORD'
        
    def sql_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(
                    host = self.host_name, 
                    user = self.user_name, 
                    password = self.password)
            print("MySQL connection successful")
        except Error as err:
            print(f"Error: '{err}'")
        return connection
    
    def create_db_connection(self, database_name):
        self.database_name = database_name
        self.connection = None
        try:
            self.connection = mysql.connector.connect(
                    host = self.host_name,
                    user = self.user_name,
                    password = self.password,
                    database = self.database_name)
            print("MySQL Database connection successful")
        except Error as err:
            print(f"Error: '{err}'")
        return self.connection  

    def execute_quote_query(self, connection, val):
        connect = connection
        sql = """
        INSERT INTO data (id, adp_100, adp_200, adp_50, adv_21, 
        adv_30, adv_90, ask, ask_time, asksz, basis, beta, bid, bid_time, 
        bidsz, bidtick, chg, chg_sign, chg_t, cl, contract_size, cusip, date_, 
        datetime_, days_to_expiration, dividend, divexdate, divfreq, divpaydt, 
        dollar_value, eps, exch, exch_desc, hi, iad, idelta, igamma, 
        imp_volatility, incr_vl, irho, issue_desc, itheta, ivega, last_, 
        lo, name_, op_delivery, op_flag, op_style, op_subclass, openinterest, 
        opn, opt_val, pchg, pch_sign, pcls, pe, phi, plo, popn, pr_adp_100, 
        pr_adp_200, pr_adp_50, pr_date, pr_openinterest, prbook, prchg, 
        prem_mult, put_call, pvol, qcond, rootsymbol, secclass, sesn, sho, 
        strikeprice, symbol, tcond, timestamp_, tr_num, tradetick, trend, 
        under_cusip, undersymbol, vl, volatility12, vwap, wk52hi, wk52hidate, 
        wk52lo, wk52lodate, xdate, xday, xmonth, xyear, yeild) 
        VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor = connect.cursor()
        try:
            cursor.execute(sql, val)
            connect.commit()
            print("Query Successful")
        except Error as err:
            print(f"Error: '{err}'")
            
    def execute_options_query(self, connection, options_quote_list):
        connect = connection
        sql = """
        INSERT INTO options_data (id, ask, ask_time, asksz, basis, bid, bid_time, 
        bidsz, chg, chg_sign, chg_t, cl, contract_size, date_, datetime_, 
        days_to_expiration, exch, exch_desc, hi, incr_vl, issue_desc, last_, lo, 
        op_delivery, op_flag, op_style, op_subclass, opn, pchg, pchg_sign, 
        pcls, phi, plo, popn, pr_date, pr_openinterest, prchg, prem_mult, 
        put_call, pvol, rootsymbol, secclass, sesn, strikeprice, symbol, 
        tcond, timestamp_, tr_num, tradetick, under_cusip, undersymbol, vl, 
        vwap, wk52hi, wk52hidate, wk52lo, wk52lodate, xdate, xday, xmonth, 
        xyear, imp_Volatility, idelta, igamma, itheta, ivega, irho, openinterest) 
        VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor = connect.cursor()
        for quote in options_quote_list:
            try:
                cursor.execute(sql, quote)
                connect.commit()
                print("Query Successful")
            except Error as err:
                print(f"Error: '{err}'")

    def execute_query(self, connection, query):
        self.connection = connection
        self.query = query
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(self.query)
            self.connection.commit()
        except Error as err:
            print(f"Error: '{err}'")
        return self.cursor.fetchall
            
class Clean:
    
    def __init__(self):
        self.tup_list = []
    def cleanQuotes(self, quotes):
        self.quotes = quotes
        for quote in quotes:
            tup = tuple(quote.values())
            self.tup_list.append(tup)
        return self.tup_list
            
    def cleanOptions(self, options):
        self.options = options
        for quote in options:
            tup = tuple(quote.values())
            self.tup_list.append(tup)
        return self.tup_list
        
        
class Save:
    
    def __init__(self):
        self.connection = SQL().create_db_connection('ticker')
    
    def saveQuotes(self, clean_quotes):
        connect = self.connection
        cursor = connect.cursor()
        cursor.execute('SELECT max(id) FROM ticker.data;')
        max_id = cursor.fetchall()
        max_id = max_id[0][0]
        if type(max_id) == int:
            max_id = max_id + 1
        else:
            max_id = 0
        for quote in clean_quotes:
            val = ((max_id), ) + quote
            SQL().execute_quote_query(connect, val)
            max_id += 1
   
    def saveOptions(self, clean_options):
        connect = self.connection
        cursor = connect.cursor()
        cursor.execute('SELECT max(id) FROM ticker.options_data;')
        max_id = cursor.fetchall()
        max_id = max_id[0][0]
        if type(max_id) == int:
            max_id = max_id + 1
        else:
            max_id = 0
        options_quote_list = []
        for quote in clean_options:
            val = ((max_id), ) + quote
            options_quote_list.append(val)
            max_id += 1
        SQL().execute_options_query(connect, options_quote_list)

"""
AllyAPI()
"""
market_status = AllyAPI().marketStatus()
quotes = AllyAPI().getQuotes()
options = AllyAPI().getOptions()
"""
SQL()
"""
connection = SQL().create_db_connection('ticker')
"""
CLean()
"""
clean_quotes = Clean().cleanQuotes(quotes)
clean_options = Clean().cleanOptions(options)
"""
Save()
"""
Save().saveQuotes(clean_quotes)
Save().saveOptions(clean_options)