
from datetime import date
from datetime import timedelta
from datetime import date, datetime, timedelta

today = date.today()
yesterday = today -  timedelta(1)



ddff19 = spark.sql("""
      select financier_outlet_cd  lv_outlet_cd,
              am.mul_dealer_cd dealer_cd,
              am.for_cd,
              am.outlet_cd
              
      
       FROM am_dealer_loc am
         JOIN dmsd_ew_fin ew 
         ON ew.Dealer_cd=am.mul_dealer_cd
         and ew.for_Cd = am.for_cd
         and ew.outlet_Cd=am.outlet_cd
         
    JOIN ddff1 ON  am.parent_group=ddff1.lv_parent_group
             AND am.mul_dealer_cd=ddff1.mul_dealer_cd
             AND am.for_cd=ddff1.for_cd
             AND am.outlet_cd=ddff1.outlet_cd
   
         WHERE ason_date = {0}
         and ew.financier_delr_catg = 'VCF'
      """.format(yesterday))


##("SELECT col1 from table where col2>{0} limit {1}".format(var2,q25))

##q25 = 500var2 = 50Q1 = spark.sql("SELECT col1 from table where col2>{0} limit {1}".format(var2,q25))