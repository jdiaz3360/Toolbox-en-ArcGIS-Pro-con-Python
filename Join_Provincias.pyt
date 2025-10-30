# -*- coding: utf-8 -*-
"""
Join_Provincia.pyt
------------------
Toolbox personalizada para unir un archivo CSV con el feature class de provincias
de República Dominicana, manejando automáticamente la codificación del CSV.
"""

import arcpy
import os
import pandas as pd

class Toolbox(object):
    def __init__(self):
        self.label = "Josue Tools"
        self.alias = "josuetools"
        self.tools = [JoinCSVProvincias]

# ===============================================================
class JoinCSVProvincias(object):
    """Une un archivo CSV con las provincias de República Dominicana"""
    def __init__(self):
        self.label = "Join CSV con Provincias RD"
        self.description = (
            "Une los datos de población desde un archivo CSV con las provincias "
            "de República Dominicana, generando una capa final con los campos "
            "Hombres, Mujeres y Total. Incluye detección automática de encoding."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        in_fc = arcpy.Parameter(
            displayName="Capa de provincias (entrada)",
            name="in_provincias",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        csv_file = arcpy.Parameter(
            displayName="Archivo CSV de población",
            name="in_csv",
            datatype="DEFile",
            parameterType="Required",
            direction="Input"
        )
        csv_file.filter.list = ["csv"]

        out_gdb = arcpy.Parameter(
            displayName="Geodatabase de salida",
            name="out_gdb",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input"
        )

        out_fc = arcpy.Parameter(
            displayName="Capa de salida (derivada)",
            name="out_fc",
            datatype="GPFeatureLayer",
            parameterType="Derived",
            direction="Output"
        )
        out_fc.parameterDependencies = [in_fc.name]
        out_fc.schema.clone = True

        return [in_fc, csv_file, out_gdb, out_fc]

    def isLicensed(self):
        return True

    def execute(self, parameters, messages):
        # === Parámetros ===
        in_fc = parameters[0].valueAsText
        csv_path = parameters[1].valueAsText
        out_gdb = parameters[2].valueAsText

        arcpy.env.workspace = out_gdb
        arcpy.env.overwriteOutput = True
        messages.addMessage(f"📁 Workspace configurado en: {out_gdb}")

        # === Leer CSV probando varios encodings ===
        df = None
        encodings = ["utf-8", "latin1", "cp1252"]
        for enc in encodings:
            try:
                df = pd.read_csv(csv_path, encoding=enc)
                messages.addMessage(f"✅ CSV leído correctamente con encoding → {enc}")
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            raise ValueError("❌ No se pudo leer el CSV con los encodings probados.")

        # Limpiar texto
        if "Provincia" not in df.columns:
            raise ValueError("❌ El CSV no contiene una columna llamada 'Provincia'.")
        df["Provincia"] = df["Provincia"].astype(str).str.strip().str.upper()

        # Guardar versión limpia
        temp_csv = os.path.join(out_gdb, "Sexo_por_Poblacion_LIMPIO.csv")
        df.to_csv(temp_csv, index=False, encoding="utf-8")

        # === Convertir CSV a tabla en la GDB ===
        tabla_temp = os.path.join(out_gdb, "Tabla_Poblacion")
        if arcpy.Exists(tabla_temp):
            arcpy.management.Delete(tabla_temp)

        arcpy.conversion.TableToTable(temp_csv, out_gdb, "Tabla_Poblacion")
        messages.addMessage(f"✅ Tabla creada correctamente: {tabla_temp}")

        # === Crear capa temporal y aplicar Join ===
        layer_temp = "Provincia_Layer"
        if arcpy.Exists(layer_temp):
            arcpy.management.Delete(layer_temp)
        arcpy.management.MakeFeatureLayer(in_fc, layer_temp)

        campo_fc = "NOM_PRO"
        campo_tabla = "Provincia"
        arcpy.management.AddJoin(layer_temp, campo_fc, tabla_temp, campo_tabla, "KEEP_COMMON")

        # === Exportar resultado ===
        out_fc_path = os.path.join(out_gdb, "Provincias_Poblacion")
        if arcpy.Exists(out_fc_path):
            arcpy.management.Delete(out_fc_path)

        arcpy.conversion.FeatureClassToFeatureClass(layer_temp, out_gdb, "Provincias_Poblacion")
        arcpy.management.Delete(layer_temp)

        messages.addMessage(f"🎉 Unión completada correctamente en: {out_fc_path}")
        parameters[3].value = out_fc_path
        return