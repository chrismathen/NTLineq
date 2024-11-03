# 1. match NTL with pop data yearwise -------------------------------------
                                              # selection of file by year
var1<-c(2016:2021)                                                # selection of file by year for extended VIIRS

datamerge<-function(var1){
  reduce(map(files[grepl({var1},files)==T],read.csv),
         inner_join)%>%                                           # inner_join is by  common id by = c("id", "left", "top", "right", "bottom", "GID_1", "NAME_1", "GID_2", "NAME_2", "HR_cphs", "geometry")
    filter(if_all(contains(as.character(var1)), ~ (.x!=0)))       # non zero pixel selection
}


mainmergefunc<-function(var){
  plan(multisession, workers = 5)
  filesread3<-future_map(var1,datamerge)
  reduce(filesread3,full_join)                ##full join as observation with na is different for each year.
}


narem <- function(df,var1,var2) {
  df %>% 
    drop_na(all_of(var1),all_of(var2))
}




# 2.compute ineq ----------------------------------------------------------




gini_hr_ntl<-function(df,condvar,hr,NTL,Population){
  cbind.data.frame("year1"=df %$%unique(year1),"HR"=hr,
                   "gini_ntl_pc_wtd"=df %>% filter(get(condvar)==hr) %$%
                     gini.wtd(get(NTL)/get(Population),get(Population))
  )
}


