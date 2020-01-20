library(forecast)
library(neuralnet)
library(markovchain)
library(tictoc)
#3665399781180224000
makeHashLists<-function()
{
    for(i in detectlist)
    {
        si<-as.character(i)
        temp<-histdat[which(histdat$DetectorId==i),]
        histdata[[si]]<-temp[order(temp$DetectorId,temp$Timestamp),]
    }
  linkdat$linkId <- as.character(linkdat$linkId)
    for(i in linkidlist)
    {
        si<-as.character(i)
        temp<-linkdat[which(linkdat$linkId==i),]
        linkdata[[si]]<-temp[order(temp$linkId,temp$Timestamp),]
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


#----Build-in funtions----#
##long-term time_series prediction
long_ts_update<-function(startt,histdatafm){
  startt<-match(startt,histdatafm$Timestamp)
  datmp<-histdatafm[c((startt-2016):(startt-1)),]
  ss1<-na.omit(datmp[which(datmp$DayOfWeek==2),]$Speed)
  sss1<-c()
  for(i in c(1:288)){
    sss1<-c(sss1,mean(ss1[i],ss1[i+288],ss1[i+288*2],ss1[i+288*3],ss1[i+288*4]))}
  ss1<-smooth(na.omit(sss1))
  ss1 <- ts(rep(na.omit(ss1),5),frequency=length(ss1)) 
  pp1<-auto.arima(ss1,ic="aic",D=1)
  #tic("long-ts-model wkd")
  #pp1<-tbats(ss1,seasonal.periods =c(288))
  spd_forecastwkday<-forecast(pp1, h=288, level = c(99.5))
  #toc()
  datmp<-histdatafm[c((startt-2592):(startt-1)),]
  ss2<-na.omit(datmp[which(datmp$DayOfWeek==1),]$Speed)
  sss2<-c()
  for(i in c(1:288)){
    sss2<-c(sss2,mean(ss2[i],ss2[i+288]))}
  ss2<-smooth(na.omit(sss2))
  ss2 <- ts(rep(na.omit(ss2),2),frequency=length(ss2))
  #pp1<-auto.arima(ss1,ic="aic",D=1)
  #tic("long-ts-model wend")
  pp2<-auto.arima(ss2,ic="aic",D=1)
  spd_forecastwkend<-forecast(pp2, h=288, level = c(99.5))
  #toc()
  return(c(rep(spd_forecastwkday$mean,5),rep(spd_forecastwkend$mean,2)))
} 
##special event time_series prediction
spevt_update<-function(dattem,detecid){
  dattemm<-dattem[which(dattem$DetectorId==detecid),]
  #startt<-match(startt,dattemm$Timestamp)
  #datmp<-dattemm[c((startt-8064):(startt-1)),]
  #ss1<-na.omit(dattemm[which(dattemm$SpecialEvents==1),]$Speed)
  ss1<-na.omit(dattemm$Speed)
  ss1<-smooth(ss1, "3RSR")
  ss1 <- ts(ss1,frequency=288) 
  #pp1<-auto.arima(ss1,ic="aic",D=1)
  pp1<-tbats(ss1,seasonal.periods =c(288))
  spd_forecastspevt<-forecast(pp1, h=288, level = c(99.5))
  return(spd_forecastspevt$mean)
} 
#prediction the upcoming week using the INRIX data for links in "TmcLinks"#
long_ts_update_tmc<-function(startt,inrixdata){
  startt<-match(startt,inrixdata$measurement_tstamp)
  datmp<-inrixdata[c((startt-2016):(startt-576)),]
  ss1<-na.omit(datmp$speed)
  ss1<-smooth(ss1)
  ss1 <- ts(ss1,frequency=288) 
  #pp1<-auto.arima(ss1,ic="aic",D=1)
  #tic("long-ts-model wkd")
  pp1<-tbats(ss1,seasonal.periods =c(288))
  spd_forecastwkday<-forecast(pp1, h=1440, level = c(99.5))
  #toc()
  datmp<-inrixdata[c((startt-2592):(startt-2016),(startt-576):startt),]
  ss2<-na.omit(datmp$speed)
  ss2 <- ts(ss2,frequency=288)
  ss2<-smooth(ss2)
  #pp1<-auto.arima(ss1,ic="aic",D=1)
  tic("long-ts-model wend")
  pp2<-tbats(ss2,seasonal.periods =c(288))
  spd_forecastwkend<-forecast(pp2, h=576, level = c(99.5))
  toc()
  #plot(spd_forecastwkday$mean)
  return(c(spd_forecastwkday$mean,spd_forecastwkend$mean))
} 

##short-term and external events predictions, generates a list of speed predictions
pred<-function(horz,startt,histdatafm,long_ts_predfm){
  dattemp<-histdatafm
  long_ts_predd<-long_ts_predfm
  if(is.element(detecid,detectlist)){
    spevt_predd<-spevt_pred[which(spevt_pred$detectid==detecid),]
  }
    intvl=horz/5
  rel<-c()
  indexx<-match(startt,dattemp$Timestamp)
  
  predspd<-c()
  predspdd<-c()
  xxx<-indexx
  tempsp<-dattemp[xxx-1,]$Speed
  spmean<-mean(na.omit(dattemp[c((xxx-287):(xxx-1)),]$Speed))
  msss<-fitS65$centers
  for (i in c(xxx:(xxx+intvl-1))){
    currenttime=dattemp[i,]$Timestamp
    tempid=match(currenttime,long_ts_predd$timestamplist)
    if(!is.na(tempid)){spd_forecastt=long_ts_predd[tempid,]$speed}
    else{spd_forecastt=200}
    
    if(dattemp[i,]$SpecialEvents==1){
      tempid=match(substr(currenttime,12,16),spevt_predd$timestamplist)
      if(!is.na(tempid)){spd_forecastt=spevt_predd[tempid,]$speed}
      else{spd_forecastt=200}
    }
    
    
    if(dattemp[i,"IncidentOnLink"]==0 && dattemp[i,"Precipication"]<=2 && dattemp[i,"IncidentDownstream"]==0 
       && dattemp[i,"WorkzoneDownstream"]==0 && dattemp[i,"WorkzoneOnLink"]==0){
      if ( any(dattemp[c((i-1):(i-4)),"IncidentOnLink"]==1) || any(dattemp[c((i-1):(i-4)),"IncidentDownstream"]==1) ||
           any(dattemp[c((i-1):(i-4)),"WorkzoneOnLink"]==1) ){     
        tempsp<-(6*spmean+tempsp)/7
        predspd<-c(predspd,tempsp)
        
      } 
      else{
        set.seed(i)
        s3<-na.omit(c(dattemp[c((xxx-120):(xxx-1)),]$Speed,predspd))
        s3<-ts(s3)
        p3<-auto.arima(s3,ic="aic")
        spd_forecast3 <-forecast(p3, h=1, level = c(99.5))
        if(spd_forecastt>(spmean-10)){
          tempsp<-spd_forecast3$mean
          if(tempsp<10 || tempsp>spmean+10){
            if(length(predspd)==0){ tempsp<-dattemp[xxx-1,]$Speed }
            else{tempsp<-predspd[length(predspd)] }
          }
          }
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
        if(tempsp>spmean+10){
          if(length(predspd)==0){ tempsp<-dattemp[xxx-1,]$Speed }
          else{tempsp<-predspd[length(predspd)] }
        }
        predspd<-c(predspd,tempsp)}
    }
    else{
      
      if (dattemp[i,"IncidentOnLink"]==1 ){
        pred<-predict(zp,dattemp[(xxx-4):xxx,],type = "class")
        t <- table(pred)
        if(as.numeric(rep(names(t)[t == max(t)],5))==1){
          
          if(is.element(dattemp[i,"LanesClosedOnLink"],c(0,1,2,3))){
            if(dattemp[i,"Precipication"]==1){mc<-mcXiol.1.1}
            else{ mc<-mcXiol.1.1 }}
          else {
            if(dattemp[i,"Precipication"]==1){mc<-mcXiol.2.1}
            else{ mc<-mcXiol.2.1 }}
          for(i in 1:4){
            stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
            mss<-fitS65$centers
            mss[stt]<-tempsp
            tempsp<-round(sum(mc[stt,]*mss),0)
            predspd<-c(predspd,tempsp)}
          
        } else{
          s3<-na.omit(c(dattemp[c((xxx-36):(xxx-1)),]$Speed,predspd))
          s3<-ts(s3)
          p3<-auto.arima(s3,ic="aic")
          spd_forecast3 <-forecast(p3, h=1, level = c(99.5))
          tempsp<-spd_forecast3$mean
          if(spd_forecastt>(spmean-10)){
            tempsp<-spd_forecast3$mean
            if(tempsp<10 || tempsp>spmean+10){
              if(length(predspd)==0){ tempsp<-dattemp[xxx-1,]$Speed }
              else{tempsp<-predspd[length(predspd)] }
            }}
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
          if(tempsp<10 || tempsp>spmean+10){
            if(length(predspd)==0){ tempsp<-dattemp[xxx-1,]$Speed }
            else{tempsp<-predspd[length(predspd)] }
          }
          predspd<-c(predspd,tempsp)
        }}
      else {
        if(dattemp[i,"IncidentDownstream"]==1){
          pred<-predict(zp,dattemp[(xxx-4):xxx,],type = "class")
          t <- table(pred)
          if(as.numeric(rep(names(t)[t == max(t)],5))==1){
            if(is.element(dattemp[i,"LanesClosedOnLink"],c(0,1,2,3))){
              if(dattemp[i,"Precipication"]==1){mc<-mcXid.1.1}
              if(is.element(dattemp[i,"Precipication"],c(2,3,5))){mc<-mcXid.1.1}
              else{ mc<-mcXid.1.1 }}
            else {
              if(dattemp[i,"Precipication"]==1){mc<-mcXid.2.1}
              if(is.element(dattemp[i,"Precipication"],c(2,3,5))){mc<-mcXid.2.1}
              else{ mc<-mcXid.2.1 }}
            
            for (mi9 in c(1:4)){
              stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
              mss<-fitS65$centers
              mss[stt]<-tempsp
              tempsp<-sum(mc[stt,]*mss)}
            predspd<-c(predspd,tempsp)
          } else{s3<-na.omit(c(dattemp[c((xxx-120):(xxx-1)),]$Speed,predspd))
          s3<-ts(s3)
          p3<-auto.arima(s3,ic="aic")
          spd_forecast3 <-forecast(p3, h=1, level = c(99.5))
          tempsp<-spd_forecast3$mean
          if(spd_forecastt>(spmean-10)){
            tempsp<-spd_forecast3$mean
            if(tempsp<10 || tempsp>spmean+10){
              if(length(predspd)==0){ tempsp<-dattemp[xxx-1,]$Speed }
              else{tempsp<-predspd[length(predspd)] }
            }}
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
          if(tempsp<10 || tempsp>spmean+10){
            if(length(predspd)==0){ tempsp<-dattemp[xxx-1,]$Speed }
            else{tempsp<-predspd[length(predspd)] }
          }
          predspd<-c(predspd,tempsp)}
        }
        else {
          if (dattemp[i,"Precipication"]>2) {
            pred<-predict(zp,dattemp[(xxx-4):xxx,],type = "class")
            t <- table(pred)
            if(as.numeric(rep(names(t)[t == max(t)],5))==1){
              if (is.element(dattemp[i,"Precipication"],c(2,3,5))) {
                if (dattemp[i,"Precipication"]==5 && any(dattemp[(i-6):i,"Precipication"]>5)){
                  mc<-mcXw3b
                }
                else{
                  mc<-mcXw5}} 
              if (is.element(dattemp[i,"Precipication"],c(4))) {mc<-mcXw2}
              if (is.element(dattemp[i,"Precipication"],c(6,7))) {mc<-mcXw3}
              
              msss<-fitS65$centers
              
              stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
              mss<-fitS65$centers
              mss[stt]<-tempsp
              tempsp<-(sum(mc[stt,]*mss)+tempsp)/2
              
              predspd<-c(predspd,tempsp)}
            else{
              s3<-na.omit(c(dattemp[c((xxx-120):(xxx-1)),]$Speed,predspd))
              s3<-ts(s3)
              p3<-auto.arima(s3,ic="aic")
              spd_forecast3 <-forecast(p3, h=1, level = c(99.5))
              tempsp<-spd_forecast3$mean
              if(spd_forecastt>(spmean-10)){
                tempsp<-spd_forecast3$mean
                if(tempsp<10 || tempsp>spmean+10){
                  if(length(predspd)==0){ tempsp<-dattemp[xxx-1,]$Speed }
                  else{tempsp<-predspd[length(predspd)] }
                }}
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
          else{ 
            if(dattemp[i-1,"WorkzoneOnLink"]==1 && dattemp[i,"LanesClosedOnLink"]>1){
              mc<-mcXiol.1.1
              for (rf8 in c(1:5)){
                stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
                mss<-fitS65$centers
                mss[stt]<-tempsp
                tempsp<-sum(mc[stt,]*mss)}
              predspd<-c(predspd,tempsp)
            }
            else{
              
              set.seed(i)
              s3<-na.omit(c(dattemp[c((xxx-120):(xxx-1)),]$Speed,predspd))
              s3<-ts(s3)
              p3<-auto.arima(s3,ic="aic")
              spd_forecast3 <-forecast(p3, h=1, level = c(99.5))
              if(spd_forecastt>(spmean-10)){
                tempsp<-spd_forecast3$mean
                if(tempsp<10 || tempsp>spmean+10){
                  if(length(predspd)==0){ tempsp<-dattemp[xxx-1,]$Speed }
                  else{tempsp<-predspd[length(predspd)] }
                }}
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
              if(tempsp<0){tempsp=0}
              predspd<-c(predspd,tempsp)}}
        }}}}
  
  rel<-c(rel,predspd)
  predspd<-c()
  return(rel)
  
  #   xxx<-indexx
  #   #rell2<-loess(rel2~c(1:length(rel2)),span=0.1)$fitted
  #   rel<-loess(rel~c(1:length(rel)),span=0.1)$fitted
  #   plot(c((xxx):(xxx+hourlen*12-1)),dattemp$Speed[(xxx):(xxx+hourlen*12-1)],ylab="Speed (km/h)",xlab="Time of Day (h)",ylim=c(10,80),xaxt = "n",main=paste(hourlen,"-hour Prediction (",textt,") of Detector",detecid ),pch=18,cex=0.7,cex.main=1.8,cex.lab=1.8)
  #   #axis(1, at=c(xxx+12*c(0:10)), labels=c(8:18),cex.axis=1.5)
  #   #lines(c((xxx):(xxx+40)),rell2[1:120],col="blue",lty=4)
  #   lines(c((xxx):(xxx+hourlen*12-1)),rel[1:(hourlen*12)],col="red",lty=4)
  #   #lines(c(xxx:(xxx+287)),spd_forecastt$mean[1:288],col="blue",lty=4)
  #   #points(c((xxx-10):(xxx+50)),dattemp$Speed[(xxx-10):(xxx+50)],col="blue")
  #   #legend("bottomleft", legend=c("Detector Speed", "Short-term Time Series","Markov-based Time Series"),
  #   #       col=c("black", "Blue","Red"), pch = c(18,NA,NA),lty=c(NA,4,4),cex=1.1)
  #   cat("error rate:",round(mean(abs((dattemp$Speed[(xxx):(xxx+hourlen*12-1)]-rel[1:(hourlen*12)])/dattemp$Speed[(xxx):(xxx+hourlen*12-1)])),2))
  #   cat("RMSE:",round(sqrt(mean((dattemp$Speed[(xxx):(xxx+hourlen*12-1)]-rel[1:(hourlen*12)])^2)),2))
  #   cat("MAE:",round(mean(abs(dattemp$Speed[(xxx):(xxx+hourlen*12-1)]-rel[1:(hourlen*12)])),2))
}

predlinks<-function(horz,startt,linkid,detpred,detid)
{
  linkdataf<-linkdata[[as.character(linkid)]]
  index<-match(startt,linkdataf$Timestamp)
  tempdf<-linkdataf[index:(index+horz/5-1),]
  tempdf[,"distance"]<-rep(UpstreamLinks[which(UpstreamLinks$linkId.Up==linkid&UpstreamLinks$detList.Down==detid),"distance"],horz/5)
  tempdf[,"Speed.Down"]<-detpred
  tempdf[,"Speed"]<-rep(0,horz/5)
  tempdf<-as.data.frame(scale(tempdf[,names(mins)],center = mins, scale = maxs - mins))
  pr.nn <- compute(nn,tempdf[,parnames])
  pr.nn_ <- pr.nn$net.result*(maxs[16]-mins[16])+mins[16]
  return(pr.nn_)
}


#For links without a paired detecor downstream
predlinksnodec<-function(horz,startt,linkdatafm,long_ts_pred_tmc){
  dattemp<-linkdatafm
  long_ts_predd<-long_ts_pred_tmc
  intvl=horz/5
  
  rel<-c()
  indexx<-match(startt,dattemp$Timestamp)
  
  predspd<-c()
  predspdd<-c()
  xxx<-indexx
  tempid=match(startt,long_ts_predd$timestamplist)
  tempsp<-long_ts_predd[tempid,]$speed
  spmean<-mean(long_ts_predd[c((tempid-287):(tempid-1)),]$speed)
  msss<-fitS65$centers
  for (i in c(xxx:(xxx+intvl-1))){
    currenttime=dattemp[i,]$Timestamp
    tempid=match(currenttime,long_ts_predd$timestamplist)
    spd_forecastt=long_ts_predd[tempid,]$speed
    
    
    
    if(dattemp[i,"IncidentOnLink"]==0 && dattemp[i,"Precipication"]<=2 && dattemp[i,"IncidentDownstream"]==0 
       && dattemp[i,"WorkzoneDownstream"]==0 && dattemp[i,"WorkzoneOnLink"]==0){
      if ( any(dattemp[c((i-1):(i-4)),"IncidentOnLink"]==1) || any(dattemp[c((i-1):(i-4)),"IncidentDownstream"]==1) ||
           any(dattemp[c((i-1):(i-4)),"WorkzoneOnLink"]==1) ){     
        tempsp<-(6*spmean+tempsp)/7
        predspd<-c(predspd,tempsp)
        
      } 
      else{
        
        tempsp<-spd_forecastt
        predspd<-c(predspd,tempsp)}
    }
    else{
      
      if (dattemp[i,"IncidentOnLink"]==1 ){
        pred<-predict(zp,dattemp[(xxx-4):xxx,],type = "class")
        t <- table(pred)
        if(as.numeric(rep(names(t)[t == max(t)],5))==1){
          
          if(is.element(dattemp[i,"LanesClosedOnLink"],c(0,1,2,3))){
            if(dattemp[i,"Precipication"]==1){mc<-mcXiol.1.1}
            else{ mc<-mcXiol.1.1 }}
          else {
            if(dattemp[i,"Precipication"]==1){mc<-mcXiol.2.1}
            else{ mc<-mcXiol.2.1 }}
          for(i in 1:4){
            stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
            mss<-fitS65$centers
            mss[stt]<-tempsp
            tempsp<-round(sum(mc[stt,]*mss),0)
            predspd<-c(predspd,tempsp)}
          
        } else{
          
          tempsp<-spd_forecastt
          
          predspd<-c(predspd,tempsp)
        }}
      else {
        if(dattemp[i,"IncidentDownstream"]==1){
          pred<-predict(zp,dattemp[(xxx-4):xxx,],type = "class")
          t <- table(pred)
          if(as.numeric(rep(names(t)[t == max(t)],5))==1){
            if(is.element(dattemp[i,"LanesClosedOnLink"],c(0,1,2,3))){
              if(dattemp[i,"Precipication"]==1){mc<-mcXid.1.1}
              if(is.element(dattemp[i,"Precipication"],c(2,3,5))){mc<-mcXid.1.1}
              else{ mc<-mcXid.1.1 }}
            else {
              if(dattemp[i,"Precipication"]==1){mc<-mcXid.2.1}
              if(is.element(dattemp[i,"Precipication"],c(2,3,5))){mc<-mcXid.2.1}
              else{ mc<-mcXid.2.1 }}
            
            for (mi9 in c(1:4)){
              stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
              mss<-fitS65$centers
              mss[stt]<-tempsp
              tempsp<-sum(mc[stt,]*mss)}
            predspd<-c(predspd,tempsp)
          } else{tempsp<-spd_forecastt
          predspd<-c(predspd,tempsp)}
        }
        else {
          if (dattemp[i,"Precipication"]>2) {
            pred<-predict(zp,dattemp[(xxx-4):xxx,],type = "class")
            t <- table(pred)
            if(as.numeric(rep(names(t)[t == max(t)],5))==1){
              if (is.element(dattemp[i,"Precipication"],c(2,3,5))) {
                if (dattemp[i,"Precipication"]==5 && any(dattemp[(i-6):i,"Precipication"]>5)){
                  mc<-mcXw3b
                }
                else{
                  mc<-mcXw5}} 
              if (is.element(dattemp[i,"Precipication"],c(4))) {mc<-mcXw2}
              if (is.element(dattemp[i,"Precipication"],c(6,7))) {mc<-mcXw3}
              
              msss<-fitS65$centers
              
              stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
              mss<-fitS65$centers
              mss[stt]<-tempsp
              tempsp<-(sum(mc[stt,]*mss)+tempsp)/2
              
              predspd<-c(predspd,tempsp)}
            else{
              
              tempsp<-spd_forecastt
              
              predspd<-c(predspd,tempsp)
            }
          }
          else{ 
            if(dattemp[i-1,"WorkzoneOnLink"]==1 && dattemp[i,"LanesClosedOnLink"]>1){
              mc<-mcXiol.1.1
              for (rf8 in c(1:5)){
                stt<-match(min(abs(msss-c(tempsp))),abs(msss-c(tempsp)))
                mss<-fitS65$centers
                mss[stt]<-tempsp
                tempsp<-sum(mc[stt,]*mss)}
              predspd<-c(predspd,tempsp)
            }
            else{
              
              tempsp<-spd_forecastt
              
              predspd<-c(predspd,tempsp)}}
        }}}}
  
  rel<-c(rel,predspd)
  predspd<-c()
  return(rel)
  
  #   xxx<-indexx
  #   #rell2<-loess(rel2~c(1:length(rel2)),span=0.1)$fitted
  #   rel<-loess(rel~c(1:length(rel)),span=0.1)$fitted
  #   plot(c((xxx):(xxx+hourlen*12-1)),dattemp$Speed[(xxx):(xxx+hourlen*12-1)],ylab="Speed (km/h)",xlab="Time of Day (h)",ylim=c(10,80),xaxt = "n",main=paste(hourlen,"-hour Prediction (",textt,") of Detector",detecid ),pch=18,cex=0.7,cex.main=1.8,cex.lab=1.8)
  #   #axis(1, at=c(xxx+12*c(0:10)), labels=c(8:18),cex.axis=1.5)
  #   #lines(c((xxx):(xxx+40)),rell2[1:120],col="blue",lty=4)
  #   lines(c((xxx):(xxx+hourlen*12-1)),rel[1:(hourlen*12)],col="red",lty=4)
  #   #lines(c(xxx:(xxx+287)),spd_forecastt$mean[1:288],col="blue",lty=4)
  #   #points(c((xxx-10):(xxx+50)),dattemp$Speed[(xxx-10):(xxx+50)],col="blue")
  #   #legend("bottomleft", legend=c("Detector Speed", "Short-term Time Series","Markov-based Time Series"),
  #   #       col=c("black", "Blue","Red"), pch = c(18,NA,NA),lty=c(NA,4,4),cex=1.1)
  #   cat("error rate:",round(mean(abs((dattemp$Speed[(xxx):(xxx+hourlen*12-1)]-rel[1:(hourlen*12)])/dattemp$Speed[(xxx):(xxx+hourlen*12-1)])),2))
  #   cat("RMSE:",round(sqrt(mean((dattemp$Speed[(xxx):(xxx+hourlen*12-1)]-rel[1:(hourlen*12)])^2)),2))
  #   cat("MAE:",round(mean(abs(dattemp$Speed[(xxx):(xxx+hourlen*12-1)]-rel[1:(hourlen*12)])),2))
}  #for links in TmcLinks #for links in TmcLinks
