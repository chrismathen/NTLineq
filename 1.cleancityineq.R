library(readr)
library(tidyverse)
library(dplyr)
library(data.table)
library(stringr)
library(glue)
library(readxl)
library(dineq)
library(lubridate)
library(furrr)
library(magrittr)


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


##############################################################



getwd()

files<-paste0("filelocation/",
              list.files(path="filelocation",pattern = "*.csv"))





# Parallel reading and merging file yearwise
filesread3<-mainmergefunc(var=var1) %>% 
  dplyr::select(-c(2:11))


#reading variable by ld and sol and joining by year and other variables  after pivoting
# by = c("id", "left", "top", "right", "bottom", "GID_1", "NAME_1", "GID_2", "NAME_2", "Name", "year1")
filesread3_ld<-full_join(filesread3 %>%
                           dplyr::select(-c(contains("pop"))) %>%
                           rename_at(vars(starts_with("ld")),~str_sub(.,-4)) %>%  
                           pivot_longer(-c("Name", "layer", "path", "id", "left", "top", "right", "bottom", "geometry"),
                                        names_to = "year1", values_to = "ld"),
                         filesread3 %>%
                           dplyr::select(c("Name", "layer", "path", "id", "left", "top", "right", "bottom", "geometry"),
                                         (contains("pop"))) %>% 
                           dplyr::select(-c(contains("sol"))) %>% #comment if error
                           rename_at(vars(starts_with("pop")),~str_sub(.,-4)) %>%  
                           pivot_longer(-c("Name", "layer", "path", "id", "left", "top", "right", "bottom", "geometry"),
                                        names_to = "year1", values_to = "popld")
) %>% 
  mutate(NAME_0="India")


#converting to list by year
filesreadld<-split(filesread3_ld,filesread3_ld$year1)

filesreadld<-map(.x=filesreadld,~narem(df=.x,var1="ld",var2="popld"))



#CITY inequality

#extracting relevant city
city<-unique(filesread3$Name)


##gini


ineq_gini_ld<-pmap_df(list(.x=rep(filesreadld,times=length(city)),.y=rep(city,each=length(filesreadld))),
                      ~gini_hr_ntl(df=.x,condvar = "Name",hr=.y,NTL="ld",Population = "popld")) %>% 
  rename(shp_city_name=HR)
  






