library(forecast)

histdata<-new.env()
long_ts_pred<-new.env()

makeHashLists<-function()
{
    for(i in detectlist)
    {
        si<-as.character(i)
        temp<-histdat[which(histdat$DetectorId==i),]
        histdata[[si]]<-temp[order(temp$DetectorId,temp$Timestamp),]
    }
}

makeHashListWeekly<-function()
{
    for(i in detectlist)
    {
        si<-as.character(i)
        temp<-histdat[which(histdat$DetectorId==i),]
        histdata[[si]]<-temp[order(temp$DetectorId,temp$Timestamp),]
    }
}

long_ts_update<-function(startt,histdatafm){
  startt<-match(startt,histdatafm$Timestamp)
  datmp<-histdatafm[c((startt-2016):(startt-1)),]
  ss1<-na.omit(datmp[which(datmp$DayOfWeek==2),]$Speed)  # select all weekdays from the last 7 days
  sss1<-c()
  for(i in c(1:288)){
    sss1<-c(sss1,mean(c(ss1[i],ss1[i+288],ss1[i+288*2],ss1[i+288*3],ss1[i+288*4])))}
  ss1<-smooth(na.omit(sss1))
  ss1 <- ts(rep(na.omit(ss1),5),frequency=length(ss1))
  pp1<-auto.arima(ss1,ic="aic",D=1)
  spd_forecastwkday<-forecast(pp1, h=288, level = c(99.5))

  datmp<-histdatafm[c((startt-2016):(startt-1)),]
  ss2<-na.omit(datmp[which(datmp$DayOfWeek==1),]$Speed)   # select all weekend days from the last 7 days
  sss2<-c()
  for(i in c(1:288)){
    sss2<-c(sss2,mean(c(ss2[i],ss2[i+288])))}
  ss2<-smooth(na.omit(sss2))
  ss2 <- ts(rep(na.omit(ss2),2),frequency=length(ss2))
  pp2<-auto.arima(ss2,ic="aic",D=1)
  spd_forecastwkend<-forecast(pp2, h=288, level = c(99.5))

  wknd_idx = match(datmp[which(datmp$DayOfWeek==1),'Timestamp'][1],datmp[,'Timestamp'])
  if(wknd_idx==1){
    long_big = c(rep(spd_forecastwkend$mean,2),rep(spd_forecastwkday$mean,5))
  }else if(wknd_idx==289){
    long_big = c(rep(spd_forecastwkday$mean,1),rep(spd_forecastwkend$mean,2),rep(spd_forecastwkday$mean,4))
  }else if(wknd_idx==577){
    long_big = c(rep(spd_forecastwkday$mean,2),rep(spd_forecastwkend$mean,2),rep(spd_forecastwkday$mean,3))
  }else if(wknd_idx==865){
    long_big = c(rep(spd_forecastwkday$mean,3),rep(spd_forecastwkend$mean,2),rep(spd_forecastwkday$mean,2))
  }else if(wknd_idx==1153){
    long_big = c(rep(spd_forecastwkday$mean,4),rep(spd_forecastwkend$mean,2),rep(spd_forecastwkday$mean,1))
  }else {
    long_big = c(rep(spd_forecastwkday$mean,5),rep(spd_forecastwkend$mean,2))
  }

  return(long_big)
}

pred_short<-function(horz,startt,histdatafm,long_ts_predfm,arima_order){
  dattemp<-histdatafm
  long_ts_predd<-long_ts_predfm
  intvl=horz/5
  rel<-c()
  indexx<-match(startt,dattemp$Timestamp)
  
  predspd<-c()
  predspdd<-c()
  xxx<-indexx
  tempsp<-dattemp[xxx-1,]$Speed
  spmean<-mean(na.omit(dattemp[c((xxx-287):(xxx-1)),]$Speed))
  msss<- centers_ohio
  msss_vsl <- centers_vsl
  msss_hsr <- centers_hsr
  for (i in seq(from = xxx, to = (xxx+intvl-1), by = 3)){
    
    # i = xxx
    currenttime=dattemp[i,]$Timestamp
    tempid=match(currenttime,long_ts_predd$timestamplist)
    if(!is.na(tempid)){
      spd_forecastt=long_ts_predd[tempid,]$speed
    }else{
      spd_forecastt=200
    }
    
    # First, determine if the contraflow ever existed in the last 5 minutes. Make sure 'contraflow' is one of the column names.
    if('contraflow' %in% colnames(dattemp) && dattemp[(xxx-1),'contraflow']>0){
      # If contraflow is implemented, determine which contraflow scenario it meets
      if(dattemp[(xxx-1),'contraflow']==1){
        if(min(dattemp[(xxx-5):xxx,'Speed']) >=20){
          mc = mc_contra[[1]]
          msss_contra = centers_contra[[1]]
        }else if(min(dattemp[(xxx-5):xxx,'Speed'])>=10&&min(dattemp[(xxx-5):xxx,'Speed'])<20){
          mc = mc_contra[[2]]
          msss_contra = centers_contra[[2]]
        }else{
          mc = mc_contra[[3]]
          msss_contra = centers_contra[[3]]
        }
      }else{
        if(min(dattemp[(xxx-5):xxx,'Speed']) >=20){
          mc = mc_contra[[4]]
          msss_contra = centers_contra[[4]]
        }else if(min(dattemp[(xxx-5):xxx,'Speed'])>=10&&min(dattemp[(xxx-5):xxx,'Speed'])<20){
          mc = mc_contra[[5]]
          msss_contra = centers_contra[[5]]
        }else{
          mc = mc_contra[[6]]
          msss_contra = centers_contra[[6]]
        }
      }
      for(i in 1:4){
        stt<-match(min(abs(msss_contra-c(tempsp))),abs(msss_contra-c(tempsp)))
        mss<-msss_contra
        mss[stt]<-tempsp
        tempsp<-round(sum(mc[stt,]*mss),2)
      }
      predspd<-c(predspd,tempsp)
      
    }else{
      # Second, determine if the vsl ever existed in the last 30 minutes
      if(length(dattemp[(xxx-4):xxx,'vsl'][dattemp[(xxx-4):xxx,'vsl']>0])>0){
        mc = markov_vsl[[1]]
        for(i in 1:4){
          stt<-match(min(abs(msss_vsl-c(tempsp))),abs(msss_vsl-c(tempsp)))
          mss<-msss_vsl
          mss[stt]<-tempsp
          tempsp<-round(sum(mc[stt,]*mss),2)
        }
        predspd<-c(predspd,tempsp)
      }else if(length(dattemp[(xxx-4):xxx,'hsr'][dattemp[(xxx-4):xxx,'hsr']>0])>0){ # Third, determine if the hsr existed in the last 30 minutes
        mc = markov_hsr[[1]]
        for(i in 1:4){
          stt<-match(min(abs(msss_hsr-c(tempsp))),abs(msss_hsr-c(tempsp)))
          mss<-msss_hsr
          mss[stt]<-tempsp
          tempsp<-round(sum(mc[stt,]*mss),2)
        }
        predspd<-c(predspd,tempsp)
      }else{
        # Lastly, if no vsl nor hsr during the last 30 minutes, then determine the scenario it belongs to
        ## base scenario: incidentonlink == 0 and incidentdownstream == 0
        if(dattemp[i,"IncidentOnLink"]==0 && dattemp[i,"IncidentDownstream"]==0){
          if ((any(dattemp[c((i-1):(i-4)),"IncidentOnLink"]==1) || any(dattemp[c((i-1):(i-4)),"IncidentDownstream"]==1) ||
               any(dattemp[c((i-1):(i-4)),"WorkzoneOnLink"]==1)) && dattemp[i,"Precipitation"]==1){     
            tempsp<-round((6*spmean+tempsp)/7,2)
            predspd<-c(predspd,tempsp)
          }else if(dattemp[i,"Precipitation"]==1){
            
            set.seed(i)
            s3<-na.omit(c(dattemp[c((xxx-72):(xxx-1)),]$Speed,predspd))
            s3<-ts(s3)
            
            p3<-arima(s3,order = arima_order)
            spd_forecast3 <-forecast(p3, h=1, level = c(99.5))
            tempsp<-round(spd_forecast3$mean,2)
            if(spd_forecastt>(spmean-10)){
              tempsp<-round(spd_forecast3$mean,2)
            }else{
              if(length(predspd)==0 ){
                if(intvl==1){
                  tempsp<-spd_forecastt-(long_ts_predd[tempid-1,]$speed-dattemp[(xxx-1),]$Speed)}
                else{
                  tempsp<-spd_forecastt
                }
              }
              else{
                tempsp<-spd_forecastt-(predspd[1]-dattemp[(xxx-1),]$Speed)}
            }
            predspd<-c(predspd,tempsp)
          } else if(dattemp[i,"Precipitation"]>1){
            pred<-predict(tree2,dattemp[(xxx-4):xxx,],type = "class")
            t <- table(pred)
            if(names(t)[t == max(t)]=='Yes'){
              if(dattemp[i,"Precipitation"] %in% c(2,3,5)){
                mc <- markov_ohio[[1]]
              }else{
                mc <- markov_ohio[[2]]
              }
              
              for(i in 1:4){
                stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
                mss<-msss
                mss[stt]<-tempsp
                tempsp<-round(sum(mc[stt,]*mss),2)
              }
              
              predspd<-c(predspd,tempsp)
              
            }else{
              
              set.seed(i)
              s3<-na.omit(c(dattemp[c((xxx-72):(xxx-1)),]$Speed,predspd))
              s3<-ts(s3)
              
              p3<-arima(s3,order = arima_order)
              spd_forecast3 <-forecast(p3, h=1, level = c(99.5))
              tempsp<-round(spd_forecast3$mean,2)
              if(spd_forecastt>(spmean-10)){
                tempsp<-round(spd_forecast3$mean,2)}
              else{
                if(length(predspd)==0 ){
                  if(intvl==1){
                    tempsp<-spd_forecastt-(long_ts_predd[tempid-1,]$speed-dattemp[(xxx-1),]$Speed)}
                  else{
                    tempsp<-spd_forecastt
                  }
                }
                else{
                  tempsp<-spd_forecastt-(predspd[1]-dattemp[(xxx-1),]$Speed)}
              }
              
              predspd<-c(predspd,tempsp)
              
            }
          }
        } else{ ## below are scenarios where incidentonlink == 1 or incidentdownstream == 1
          pred<-predict(tree2,dattemp[(xxx-4):xxx,],type = "class")
          t <- table(pred)
          if(names(t)[t == max(t)]=='Yes'){
            if(dattemp[i,"IncidentOnLink"]==1){
              
              if(dattemp[i,"Precipitation"]==1 && dattemp[i,"LanesClosedOnLink"]<2){
                mc <- markov_ohio[[3]]
              }else if(dattemp[i,"Precipitation"]==1 && dattemp[i,"LanesClosedOnLink"]>=2){
                mc <- markov_ohio[[4]]
              }else if(dattemp[i,"Precipitation"] %in% c(2,3,5) && dattemp[i,"LanesClosedOnLink"]<2){
                mc <- markov_ohio[[7]]
              }else if(dattemp[i,"Precipitation"] %in% c(2,3,5) && dattemp[i,"LanesClosedOnLink"]>=2){
                mc <- markov_ohio[[8]]
              }else if(dattemp[i,"Precipitation"] %in% c(4,6,7,8)){
                mc <- markov_ohio[[11]]
              }
            }else if(dattemp[i,"IncidentDownstream"]==1){
              
              if(dattemp[i,"Precipitation"]==1 && dattemp[i,"LanesClosedDownstream"]<2){
                mc <- markov_ohio[[5]]
              }else if(dattemp[i,"Precipitation"]==1 & dattemp[i,"LanesClosedDownstream"]>=2){
                mc <- markov_ohio[[6]]
              }else if(dattemp[i,"Precipitation"] %in% c(2,3,5) && dattemp[i,"LanesClosedDownstream"]<2){
                mc <- markov_ohio[[9]]
              }else if(dattemp[i,"Precipitation"] %in% c(2,3,5) && dattemp[i,"LanesClosedDownstream"]>=2){
                mc <- markov_ohio[[10]]
              }else if(dattemp[i,"Precipitation"] %in% c(4,6,7,8) ){
                mc <- markov_ohio[[12]]
              }
            }
            for(i in 1:4){
              stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
              mss<-msss
              mss[stt]<-tempsp
              tempsp<-round(sum(mc[stt,]*mss),2)
            }
            predspd<-c(predspd,tempsp)
          }else{
            
            s3<-na.omit(c(dattemp[c((xxx-72):(xxx-1)),]$Speed,predspd))
            s3<-ts(s3)
            
            p3<-arima(s3,order = arima_order)
            spd_forecast3 <-forecast(p3, h=1, level = c(99.5))
            tempsp<-round(spd_forecast3$mean,2)
            
            if(spd_forecastt>(spmean-10)){
              tempsp<-round(spd_forecast3$mean,2)}
            else{
              if(length(predspd)==0 ){
                if(intvl==1){
                  tempsp<-spd_forecastt-(long_ts_predd[tempid-1,]$speed-dattemp[(xxx-1),]$Speed)}
                else{
                  tempsp<-spd_forecastt
                }
              }
              else{
                tempsp<-spd_forecastt-(predspd[1]-dattemp[(xxx-1),]$Speed)}
            }
            
            predspd<-c(predspd,tempsp)
            
          }
        }
      }
    }
  }
  rel<-c(rel,predspd)
  predspd<-c()
  return(rel)
}

oneshot<-function(weather_intvl,startt, histdatafm,long_ts_predfm){
  dattemp<-histdatafm
  intvl=weather_intvl/5
  long_ts_agg = aggregate(long_ts_predfm, list(rep(1:(nrow(long_ts_predfm) %/% intvl + 1), 
                                                 each = intvl, len = nrow(long_ts_predfm))), mean)[-1]
  long_ts_agg$timestamplist = long_ts_predfm$timestamplist[seq(1, nrow(long_ts_predfm), intvl) ]
  rel<-c()
  predspd<-c()
  
  xxx<-match(startt,dattemp$Timestamp)
  xxx_longterm <-match(startt,long_ts_agg$timestamplist)
  msss<- centers_oneshot
  tempsp = long_ts_agg$speed[xxx_longterm]
  for (i in c(xxx:(xxx+((288*7)/intvl)-1))){
    # i = xxx + 1
    currenttime=dattemp[i,]$Timestamp
    tempid=match(currenttime,long_ts_agg$timestamplist)
    if(length(predspd)==0){
      tempsp = dattemp$Speed[xxx]
      predspd<-c(predspd,tempsp)
    }else if((dattemp[i,"Precipitation"]==1)&&any(dattemp[c((i-1):(i-5)),"Precipitation"] %in% c(2,3,5))){
      mc <- markov_oneshot_up[[1]]
      for(i in 1:2){
        stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
        mss<-msss
        mss[stt]<-tempsp
        tempsp<-round(sum(mc[stt,]*mss),3)
      }
      predspd<-c(predspd,tempsp)
    }else if((dattemp[i,"Precipitation"]==1)&&any(dattemp[c((i-1):(i-5)),"Precipitation"] %in% c(4,6,7,8))){
      mc <- markov_oneshot_up[[2]]
      for(i in 1:2){
        stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
        mss<-msss
        mss[stt]<-tempsp
        tempsp<-round(sum(mc[stt,]*mss),3)
      }
      predspd<-c(predspd,tempsp)
    }else{
      if(dattemp[i,"Precipitation"]==1 ){
        tempsp = long_ts_agg$speed[tempid]
        predspd<-c(predspd,tempsp)
      }else if(dattemp[i,"Precipitation"] %in% c(2,3,5) ){
        
        mc <- markov_oneshot[[1]]
        
        for(i in 1:2){
          stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
          mss<-msss
          mss[stt]<-tempsp
          tempsp<-round(sum(mc[stt,]*mss),3)
        }
        predspd<-c(predspd,tempsp)
      }else{
        
        mc <- markov_oneshot[[2]]
        for(i in 1:2){
          stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
          mss<-msss
          mss[stt]<-tempsp
          tempsp<-round(sum(mc[stt,]*mss),3)
        }
        predspd<-c(predspd,tempsp)
      }
    }

      }
  return(predspd)

}
