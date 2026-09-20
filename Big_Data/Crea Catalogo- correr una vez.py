# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC -- CatÃ¡logo raÃ­z del proyecto
# MAGIC CREATE CATALOG IF NOT EXISTS cee_aragonv2
# MAGIC   MANAGED LOCATION 'abfss://unity-catalog-storage@dbstoraged5qxl2xh3zhi2.dfs.core.windows.net/7405610835652520/cee_aragonv2'
# MAGIC   COMMENT 'Certificados de eficiencia energÃ©tica de AragÃ³n - proyecto Big Data';
# MAGIC
# MAGIC USE CATALOG cee_aragonv2;
# MAGIC
# MAGIC -- Las cuatro capas
# MAGIC CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Archivos tal como llegan del origen';
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Capa cruda inmutable. Todo texto, sin inferir tipos';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Datos tipados y validados, con cuarentena auditable';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Tablas preparadas para cada consumidor';
# MAGIC
# MAGIC -- Volume donde vive el CSV original
# MAGIC CREATE VOLUME IF NOT EXISTS cee_aragonv2.landing.raw
# MAGIC   COMMENT 'CSV del registro, archivado sin modificar';
# MAGIC
# MAGIC -- Volume temporal que necesita MLflow en serverless (lo crea tambiÃ©n el notebook 05)
# MAGIC CREATE VOLUME IF NOT EXISTS cee_aragonv2.landing.mlflow_tmp;
# MAGIC
# MAGIC -- ComprobaciÃ³n
# MAGIC SHOW SCHEMAS IN cee_aragonv2;