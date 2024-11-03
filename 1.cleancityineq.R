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
  





library(readxl)
cityncrb <- read_excel("Routput/cityncrb.xlsx")


ineq<-inner_join(ineq_gini_ld,cityncrb)
