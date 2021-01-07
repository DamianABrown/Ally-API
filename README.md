# Ally-API

### Objective
Download real time quote data using Ally Invest API and save data to MySQL. 

**Data**

Stock Quote Data:
Quote data from all 500 companies in the SP500. Data includes Ask/Bid price, Ask/Bid time, Ask/Bid size, moving averages, volatility, volume and more.

Stock Options Data:
Options data is limited to one options chain per request unlike quote request which can have mulitple tickers listed. Currently gathering options chain data for SPX. Data includes all available expiration datas and their associated options chain. Within each chain, data for calls and puts Ask/Bid price, open interest, volume, volatility, the greeks and more.

### Future Objective
With the ability to request market status ('open', 'close', 'pre', 'post'), I'll request quotes on a timed loop while market status is 'open' and when market status changes I'll have the script sleep until market open.

**Questions or Concerns**

Feel free to message me with any thoughts.

-Damian
