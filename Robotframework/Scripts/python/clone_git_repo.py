# Databricks notebook source
# MAGIC %sh
# MAGIC cd $HOME
# MAGIC ls

# COMMAND ----------

# rm -r $HOME/karthik

# COMMAND ----------

# mkdir $HOME/karthik

# COMMAND ----------

# MAGIC %sh
# MAGIC cd $HOME/karthik
# MAGIC git config --global user.name "Karthik.Narayan"
# MAGIC git config --global user.email "narayankarthik19@gmail.com"
# MAGIC repo_token = ""
# MAGIC user = "narayankarthik"
# git clone https://$user:$repo_token@github.com/narayankarthik/testRegression.git
# MAGIC # git -c "http.extraHeader=Authorization: Bearer $repo_token" clone "https://github.com/narayankarthik/testRegression.git"

# COMMAND ----------

# MAGIC %sh
# MAGIC cd $HOME/karthik/testRegression/Robotframework
# MAGIC ls

# COMMAND ----------

# rm -r /dbfs/FileStore/testRepo/Mainkar/

# COMMAND ----------

# MAGIC %sh cp -r $HOME/karthik/testRegression/Robotframework /dbfs/FileStore/testRepo/Mainkar/

# COMMAND ----------

# display(dbutils.fs.ls("/FileStore/testRepo/Mainkar/"))
