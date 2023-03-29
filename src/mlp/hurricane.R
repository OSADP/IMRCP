timelist <- sort(unique(substr(test_histdata$Timestamp, 12, 19)))
olddate <- as.Date(test_warntime, origin = "yyyy-m-dd")
timestamplist <- c()
for (n in c(1:7)) 
{
	for (m in timelist) 
	{
		timestamplist <- c(timestamplist, paste(olddate+(n-1), m))
	}
}
#timestamplist<-c()
#for (n in c(0:168)) 
#	timestamplist<-c(timestamplist, format(as.POSIXct(test_warntime) + 3600 * n, '%Y-%m-%d %H:%M:%S'))

linkname_all = sort(unique(test_histdata$linkid))
linkname_oneshot = sort(unique(svm_pred_test$linkid))
seven_day_oneshot<-function()
{
#  Description: The following for-loop calculates the long-term speed prediction
# for all the links in the network:
# (1)If the link is on the major freeway listed above, the long-term prediction is
# the combination of the conventional long-term and the SVM oneshot prediction.
# (2) If the link is on the freeways, the long-term prediction only contains the
# conventional long-term prediction.

# Start calculating the long-term, or 7-Day oneshot prediction
# The output is a list containing all links' 7-day speed prediction at 1-hour
# time resolution
	for(n in c(1:length(linkname_all)))
	{
		testlink = linkname_all[n]
		# testlink =  "0300d6a7176d626d45f901bf1eebf6c41266d1e77bbb63c4dab0a65d0e6923aa"
		hdata_link_test = test_histdata[which(test_histdata$linkid==testlink),]
		warnt_idx_test = match(test_warntime,hdata_link_test$Timestamp)
		if (is.na(warnt_idx_test) || warnt_idx_test < 168)
			next
		histd_bef_test = hdata_link_test[(warnt_idx_test-168):(warnt_idx_test-1),]
		# histd_dur_test = hdata_link_test[(warnt_idx_test):(warnt_idx_test+168-1),]

		# use self past 7 days for self next 7 days long-term prediction
		datmp_1 <- histd_bef_test
		ss1 <- smooth(na.omit(datmp_1$Speed))
		tts1 <- ts(rep(na.omit(ss1), 7), frequency = length(ss1))
		pp1 <- auto.arima(tts1, ic = "aic", D = 1)
		spd_forecast_long_1 <- forecast(pp1, h = 168, level = c(99.5))

		longpred_orig = spd_forecast_long_1$mean
		longpred_oneshot = longpred_orig

		# The following criteria separates the freeway links from the non-freeway links
		if(testlink %in% linkname_oneshot)
		{
			svm_pred_test_link = svm_pred_test[svm_pred_test$linkid==testlink,]
			impact_day = svm_pred_test_link$DayAftWarn[svm_pred_test_link$pred2=='Yes']

			if(length(impact_day)>0)
			{
				for(h in c(1:length(impact_day)))
				{
					mark_start = (impact_day[h]*24-23)
					mark_end = impact_day[h]*24
					spd_temp = c()
					for (k in c(1:12))
					{
						spd_temp[k] = longpred_oneshot[(mark_start+(k-1)*2):(mark_start+(k-1)*2+1)]
					}
					startspd = spd_temp[1]
					endspd = spd_temp[12]

					idx1 = match(min(abs(Spd_centers-c(startspd))),abs(Spd_centers-c(startspd)))
					min1 = round(sum(mc1[idx1,]*Spd_centers),0)
					if(idx1<=4)
					{
						min1 = min1*0.9
					}
					idx2 = match(min(abs(Spd_centers-c(endspd))),abs(Spd_centers-c(endspd)))
					min2 = round(sum(mc2[idx2,]*Spd_centers),0)
					if(idx2<=4)
					{
						min2 = min2*0.9
					}
					min_speed = min(c(min1,min2))

					min_index = which(min(longpred_oneshot[mark_start:mark_end])==longpred_oneshot[mark_start:mark_end])
					maxdiff=min(longpred_oneshot[mark_start:mark_end])[1]-min_speed
					impact_each = numeric(length(c(1:24)))
					for(bb in c(1:24))
					{
						if (bb<min_index[1])
						{
							impact_each[bb] = sqrt(abs(bb-0)/(min_index[1]-0))*maxdiff
						}
						else if(bb>min_index[length(min_index)]) 
						{
							impact_each[bb] = sqrt(abs(25-bb)/(25-min_index[length(min_index)]))*maxdiff
						}	
						else
						{
							impact_each[bb] = maxdiff
						}
					}
					longpred_oneshot[mark_start:mark_end] = longpred_oneshot[mark_start:mark_end] - impact_each
				}
			}
		}
		
		pred_oneshot[[testlink]]$Timestamp = timestamplist
		pred_oneshot[[testlink]]$Oneshot = longpred_oneshot
	}
	return(pred_oneshot)
}

pred <- function(horz,
                 startt,
                 histdatafm,
                 long_ts_predfm) {
  dattemp <- histdatafm
  long_ts_predd <- long_ts_predfm
  
  intvl = horz/1 
  rell <- c()
  indexx <- match(startt, dattemp$Timestamp)
  # indexx = match(startt,(substr(dattemp$Timestamp, 1, 16))) 
  if (is.na(indexx) || indexx < 23)
   return(rell)  
  predspd <- c()
  xxx <- indexx
  tempsp <- dattemp[xxx - 1,]$Speed
  spmean <- mean(na.omit(dattemp[c((xxx - 23):(xxx - 1)),]$Speed))
  
  for (i in seq(xxx, (xxx + intvl - 1), by = 1)) {
    currenttime = dattemp[i,]$Timestamp
    tempid = match(currenttime, long_ts_predd$Timestamp)
    # tempid = i - xxx + 1
    if (!is.na(tempid))
    {
      spd_forecastt = long_ts_predd$Oneshot[tempid]
    } else{
      spd_forecastt = 200
    }
    
    set.seed(i)
    s3 <-
      na.omit(c(dattemp[c((xxx - 12):(xxx - 1)),]$Speed, predspd))
    s3 <- ts(s3)
    p3 <- auto.arima(s3, ic = "aic")
    spd_forecast3 <- forecast(p3, h = 1, level = c(99.5))
    if (spd_forecastt > (spmean - 15)) 
    {
      tempsp <- spd_forecast3$mean
    } else{
      if (length(predspd) == 0) {
        if (intvl == 1) {
          tempsp <-
            spd_forecastt - (long_ts_predd$Oneshot[tempid] - dattemp[(xxx -1),]$Speed)
        }
        else{
          tempsp <- spd_forecastt
        }
      }
      else{
        tempsp <- spd_forecastt - (predspd[1] - dattemp[(xxx - 1),]$Speed)
      }
    }
    
    predspd <- c(predspd, tempsp)
    
  }
  rell <- c(rell, predspd)
  predspd <- c()
  return(rell)
}    
