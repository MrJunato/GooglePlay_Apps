####------------------------------------------------------------------ 0: Alunos ----------------------------------------------------------------####

## Obs: necessário ter criado o banco de dados R no SQL Server para rodar o script.
##      Begin Try Drop DataBase R; End Try Begin Catch Create DataBase R; End Catch;

####------------------------------------------------------- 1: Declaração das Bibliotecas -------------------------------------------------------####

install.packages('ca');
install.packages("odbc");
install.packages("psych");
install.packages("dplyr");
install.packages("GGally");
install.packages("igraph");
install.packages("stringr");
install.packages("cluster");
install.packages("tidyverse");
install.packages("data.table");
install.packages("factoextra");

library(ca);
library(odbc);
library(psych);
library(dplyr);
library(igraph);
library(GGally);
library(cluster);
library(ggplot2);
library(stringr);
library(cluster);
library(tidyverse);
library(data.table);
library(factoextra);

####------------------------------------------------------ 2: Conexão com o banco de dados ------------------------------------------------------####
## Conexão com Usuário e Senha
obj_odbc_conn <- DBI::dbConnect( odbc::odbc()                            ##Tipo de conexão.
                               , Driver       = "SQL Server"             ##Driver de conexão.
                               , Server       = "localhost\\SQLEXPRESS"  ##Servidor local ou Ip com ':1433' (Porta).
                               , Database     = "R"                      ##Base de Dados.
                               , User         = "wbpereir"
                               , Password     = "692456" );

## Conexão com trunsted Connection
##obj_odbc_conn <- DBI::dbConnect( odbc::odbc()                            ##Tipo de conexão.
##                               , Driver       = "SQL Server"             ##Driver de conexão.
##                               , Server       = "localhost\\SQLEXPRESS"  ##Servidor local ou Ip com ':1433' (Porta).
##                               , Database     = "R"                      ##Base de Dados.
##                               , Trusted_Connection = "True" );          ##Trusted_Connection refere-se a utilização do usuário do Windows.

####-------------------------------------------------------- 3: Set da area de trabalho ---------------------------------------------------------####
setwd("C:/Users/willc/Desktop/app");

####--------------------------------------------------------- 4: Análise de Inferência ----------------------------------------------------------####

####------------------------------------------------------------- 4.1: Import CSV ---------------------------------------------------------------####
obj_data_table_apps    <- read.csv("apps.csv",    sep = ";"); ## Arquivo Apps.
obj_data_table_reviews <- read.csv("reviews.csv", sep = ";"); ## Arquivo Reviews.

####---------------------------------------------------------- 4.2: Banco de dados --------------------------------------------------------------####
dbSendQuery(obj_odbc_conn, "Begin Try Drop Table R..tb_apps;    End Try Begin Catch End Catch;");
dbSendQuery(obj_odbc_conn, "Begin Try Drop Table R..tb_reviews; End Try Begin Catch End Catch;");

dbWriteTable(obj_odbc_conn, "tb_apps",   obj_data_table_apps,    append = FALSE); ## Cria a tabela com os dados imortados.
dbWriteTable(obj_odbc_conn, "tb_reviews",obj_data_table_reviews, append = FALSE); ## Cria a tabela com os dados imortados.

####------------------------------------------ 4.3: Recupera os dados tratados pelo banco de dados ----------------------------------------------####
str_sql <- "
Select app
     , category As categoria
     , Cast( Case When IsNumeric(rating) = 0 Then '0' Else rating End As Float ) As pontuacao
     , Cast( reviews As BigInt ) As vlm_comentarios
     , Case When Right(installs,7) = '000,000' Then Cast(Replace(installs,',','.') As Float) * 1000000
            When Right(installs,3) = '000'     Then Cast(Replace(installs,',','.') As Float) * 1000
       Else Cast( Replace(Replace(installs,'+','' ),',','.') As Float) End As instalacoes
     , [type] As tipo
     , [content.rating] As classif_categoria
     , genres As genero
From R..tb_apps ( Nolock )
Where IsNumeric(reviews) = 1 And IsNumeric(Replace( installs, ',', '.' )) = 1;
";

rs_apps <- dbFetch(dbSendQuery(obj_odbc_conn, str_sql));    ## Recupera os dados da procedure R..sp_apps.
## rs_apps <- dbFetch(dbSendQuery(obj_odbc_conn, "Exec R..sp_apps;"));    ## Recupera os dados da procedure R..sp_apps.

str_sql <- "
Select app
     , sentiment As sentimento
     , Cast( sentiment_polarity As Float ) As sentiment_polarity
     , Cast( sentiment_subjectivity As Float ) As sentiment_subjectivity
From R..tb_reviews ( Nolock )
Where     IsNumeric(Sentiment_Polarity) = 1
      And IsNumeric(Sentiment_Subjectivity) = 1;
";

rs_reviews <- dbFetch(dbSendQuery(obj_odbc_conn, str_sql)); ## Recupera os dados da procedure R..sp_reviews.
##rs_reviews <- dbFetch(dbSendQuery(obj_odbc_conn, "Exec R..sp_reviews;")); ## Recupera os dados da procedure R..sp_reviews.

####--------------------------------------------- 4.4: Agrupa os dados de sentimentos por app ---------------------------------------------------####
rs_reviews$app        <- as.factor(rs_reviews$app);        ## Converte a coluna app em fator. 
rs_reviews$sentimento <- as.factor(rs_reviews$sentimento); ## Converte a coluna sentimento em fator.

analise_reviews <- rs_reviews%>%group_by(app, sentimento)%>%
  summarise(avg_nota = mean(sentiment_polarity)); ## Agrupa os dados de sentimentos por app.

####--------------------------------------------- 4.5: Agrupa os dados de pontuacoes por app ---------------------------------------------------####
rs_apps$app <- as.factor(rs_apps$app);  ## Converte a coluna app em fator.

analise_apps <- rs_apps%>%group_by(app)%>%
  summarise(avg_nota = mean(pontuacao)); ## Agrupa os dados de pontuacoes por app.

####---------------------------------------- 4.6: Sobe os dados agrupados para o banco de dados ------------------------------------------------####
dbSendQuery(obj_odbc_conn, "Begin Try Drop Table R..tb_analise_sentimento; End Try Begin Catch End Catch;");
dbSendQuery(obj_odbc_conn, "Begin Try Drop Table R..tb_analise_pontuacoes; End Try Begin Catch End Catch;");

dbWriteTable(obj_odbc_conn, "tb_analise_sentimento", analise_reviews, append = FALSE); ## Sobe os dados agrupados para o baco de dados.
dbWriteTable(obj_odbc_conn, "tb_analise_pontuacoes", analise_apps   , append = FALSE); ## Sobe os dados agrupados para o baco de dados.

####-------------------------------------- 4.7: Cruza as tabelas com o SQL sumarizando os dados ------------------------------------------------####
str_sql <- "
Select PON.app
     , PON.avg_nota As nota
     , Round( SNU.avg_nota * 100, 2 ) As snt_neutro
     , Round( SPO.avg_nota * 100, 2 ) As snt_positivo
     , Round( SNG.avg_nota * 100, 2 ) As snt_negativo
From            R..tb_analise_pontuacoes PON (Nolock)
     Inner Join R..tb_analise_sentimento SNU (Nolock) On SNU.app = PON.app And SNU.sentimento = 'Neutral'
     Inner Join R..tb_analise_sentimento SPO (Nolock) On SPO.app = PON.app And SPO.sentimento = 'Positive'
     Inner Join R..tb_analise_sentimento SNG (Nolock) On SNG.app = PON.app And SNG.sentimento = 'Negative';
";
rs_anl_inferencia <- dbFetch(dbSendQuery(obj_odbc_conn, str_sql)); ## Recupera os dados da procedure Exec sp_segue_pontuacao_sentimento;

####------------------------------------------- 4.7: Inferência de sentimento Positivo por Nota ------------------------------------------------####
ggplot(rs_anl_inferencia, aes(x = snt_positivo, y = nota), new = T) + geom_point() +
  geom_smooth(data = rs_anl_inferencia, formula = y ~ x, colour = "red", se = FALSE, method = 'lm');

####------------------------------------------- 4.8: Inferência de sentimento Negativo por Nota ------------------------------------------------####
ggplot(rs_anl_inferencia, aes(x = snt_negativo, y = nota), new = T) + geom_point() +
  geom_smooth(data = rs_anl_inferencia, formula = y ~ x, colour = "red", se = FALSE, method = 'lm');


####-------------------------------------------------------- 5: Análise de Cluster ------------------------------------------------------------####

data = read.csv2("googleplaystore.csv", sep=",")
data %>% head

####---------------------------------------------- 5.1: Selecting the important categories ----------------------------------------------------####
category_important = data %>% group_by(Category) %>% 
  summarise (n = n()) %>% 
  arrange(desc(n)) %>% 
  mutate (per = round(cumsum(n)/sum(n),2)) %>%
  filter(per<0.7)
category_important %>% head

####-------------------------- 5.2: Create the Contigency Table (Cross Tabulation) for Correspondence Analysis --------------------------------####
# "Correspondence analysis is a statistical technique that provides a graphical representation
#  of cross tabulations  (which are also known as cross tabs, or contingency tables)" Yelland, Phillip M. 2010.
contingency_table = data %>% 
  filter(Category == category_important$Category) %>% 
  select(Category, Content.Rating) %>%
  mutate(n=1) %>%
  group_by(Category, Content.Rating) %>%
  summarise(sum = sum(n)) %>%
  spread(Content.Rating, sum)

dplyr_if_else <- function(x) { mutate_all(x, funs(if_else(is.na(.), 0, .))) }
contingency_table = dplyr_if_else (contingency_table)

contingency_table = data.frame(contingency_table)
rownames(contingency_table) = contingency_table$Category
contingency_table = contingency_table[,-1]
head(contingency_table)
# Chi-squared Test for measure association between variables
chisq.test(contingency_table)

####------------------------------------------------------------ 5.3: Ap Perceptual -----------------------------------------------------------####
# Since the value of p-value is less than the level of significance of 0.05, we reject
# the null hypothesis that the Category is independent of the Content Classification.
fviz_ca_biplot(ca(contingency_table), repel = TRUE, title="Comprehensive analysis of Contingency Table plot")

####----------------------------------------------------------- 5.4: Cluster Analysis ---------------------------------------------------------####
set.seed(123)
km.res <- kmeans(contingency_table, 3, 25)
fviz_cluster(km.res, data = contingency_table, palete=c("#2E9FDF","#00AFBB"), 
             ellipse.type="euclid", star.plot=TRUE, repel = TRUE, ggtheme = theme_minimal(),
             main="Perceptual Map of Category and Content Rating")

####-------------------------------------------------------- 5.5: Phylogenic-like Tree -------------------------------------------------------####
res.dist <- dist(contingency_table, method = "euclidean")
res.hc <- hclust( d = res.dist, method = "ward.D2")
fviz_dend(res.hc, k = 5, # Cut in four groups 
          k_colors = "jco", type = "phylogenic", 
          repel = TRUE, phylo_layout = "layout.gem")

####---------------------------------------------------------- 5.6: Cluster Analysis ---------------------------------------------------------####
set.seed(123)
km.res <- kmeans(contingency_table, 3, 25)
fviz_cluster(km.res, data = contingency_table, palete=c("#2E9FDF","#00AFBB"), 
             ellipse.type="convex", star.plot=TRUE, repel = TRUE, ggtheme = theme_minimal(),
             main="Perceptual Map of Category and Content Rating")
fviz_dend( res.hc, k = 3,  # Cut in four groups 
           cex = 0.5, # label size 
           k_colors = c("#2E9FDF", "#00AFBB", "#E7B800"), 
           color_labels_by_k = TRUE, # color labels by groups 
           rect = TRUE # Add rectangle around groups 
)
fviz_dend( res.hc, cex = 0.5, k = 3, k_colors = "jco", type = "circular")
fviz_dend( res.hc, k = 3, k_colors = "jco", type = "phylogenic", repel = TRUE, phylo_layout = "layout_with_drl")
fviz_dend( res.hc, k = 3, k_colors = "jco", type = "phylogenic", repel = TRUE, phylo_layout = "layout.mds")
fviz_dend( res.hc, k = 3, k_colors = "jco", type = "phylogenic", repel = TRUE, phylo_layout = "layout.mds")
fviz_dend( res.hc, k = 3, k_colors = "jco", type = "phylogenic", repel = TRUE, phylo_layout = "layout_with_lgl")

dbSendQuery (obj_odbc_conn, "Begin Try Drop Table R..tb_contingency; End Try Begin Catch End Catch;");
dbWriteTable(obj_odbc_conn, "tb_contingency", contingency_table, append = FALSE) ## Sobe os dados para o baco de dados.