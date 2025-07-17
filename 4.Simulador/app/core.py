# app/core.py

import pandas as pd
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pandas as pd
import os


#url = "postgresql://postgres.ixnhworgyhvqsojyocld:Lagobo20255@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
# Leer tablas desde Supabase
#data = pd.read_sql('SELECT * FROM "Data_final"', engine)
#data_pagaduria = pd.read_sql('SELECT * FROM "Fecha_pagaduria"', engine)
#df_tasa_usura = pd.read_sql('SELECT * FROM "Tasa_usura"', engine)


nro_libranza = 53114


import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# Leer URL de conexión desde variable de entorno
DATABASE_URL = 'postgresql://postgres.ixnhworgyhvqsojyocld:Lagobo20255@aws-0-us-east-1.pooler.supabase.com:6543/postgres'
url = os.environ.get("DATABASE_URL")

# Crear engine sin pool local (usa el de Supabase)
engine = create_engine(
    url,
    connect_args={"sslmode": "require"},
    poolclass=NullPool
)


# Funciones para obtener datos (cuando se necesiten)
def get_data_final(nro_libranza: int):
    query = f'''
        SELECT *
        FROM "data_final_2"
        WHERE "nolibra" = {nro_libranza}
    '''
    return pd.read_sql(query, engine)

def get_data_pagaduria():
    return pd.read_sql('SELECT * FROM "Fecha_pagaduria"', engine)

def get_tasa_usura():
    return pd.read_sql('SELECT * FROM "Tasa_usura"', engine)


def simular_libranza(nro_libranza: int) -> dict:
    
    data = get_data_final(nro_libranza) 
    data_pagaduria = get_data_pagaduria()
    df_tasa_usura = get_tasa_usura()

    #data = pd.read_excel("data/Data_final.xlsx")
    #data_pagaduria = pd.read_excel("data/Fecha_pagaduria.xlsx")
    #df_tasa_usura = pd.read_excel("data/Tasa_usura.xlsx")


    # Parametros y variables para la simulación

    # Número de libranza a consultar
    #nro_libranza = 53114
    # Obtener fecha actual
    hoy = datetime.today()
    # Obtener pagaduría según libranza
    pagaduria = data.loc[data["nolibra"] == nro_libranza, "pagaduria"].values[0]
    # Asegurar columna FECHA en formato datetime
    data_pagaduria["FECHA"] = pd.to_datetime(data_pagaduria["FECHA"])
    # Obtener fecha programada de pagaduría
    fecha_programada = pd.to_datetime(data_pagaduria.loc[data_pagaduria["PAGADURIA"] == pagaduria, "FECHA"].values[0])
    # Calcular corte_saldo_sistema
    if hoy.date() > fecha_programada.date():
        año, mes = hoy.year, hoy.month
    else:
        año, mes = (hoy.year - 1, 12) if hoy.month == 1 else (hoy.year, hoy.month - 1)
    ultimo_dia = calendar.monthrange(año, mes)[1]
    corte_saldo_sistema = datetime(año, mes, ultimo_dia)


    # Obtener datos del prestamos desde el DataFrame

    # Monto total del préstamo
    monto_prestado = data.loc[data["nolibra"] == nro_libranza, "base_capital"].values[0]
    # Tasa mensual nominal (decimal)
    tasa_mensual = data.loc[data["nolibra"] == nro_libranza, "tasa_credito"].values[0]
    # Plazo en meses
    plazo_meses = data.loc[data["nolibra"] == nro_libranza, "plazo"].values[0]
    plazo = plazo_meses  # para usar un nombre corto
    # Seguro deudor mensual
    seguro_deudor = data.loc[data["nolibra"] == nro_libranza, "seguro_deudores"].values[0] / plazo_meses
    # Fecha inicio crédito (datetime)
    fecha1 = pd.to_datetime(data.loc[data["nolibra"] == nro_libranza, "fecha_inicio_credito"].values[0])
    fecha2 = pd.to_datetime(data.loc[data["nolibra"] == nro_libranza, "fecha_primer_pago"].values[0])
    fecha_inicio = min(fecha1, fecha2)
    # Saldo cierre mes 0
    saldo_cierre_mes_0 = data.loc[data["nolibra"] == nro_libranza, "saldo_cierre_mes_0"].values[0]
    # Cuotas pagas mes
    cuotas_paga_mes = data.loc[data["nolibra"] == nro_libranza, "cuotas Pagas"].values[0]
    # Cuotas causadas calculadas
    diff = relativedelta(corte_saldo_sistema, pd.to_datetime(data.loc[data["nolibra"] == nro_libranza, "fecha_inicio_credito"].values[0]))
    cuotas_causadas = diff.years * 12 + diff.months + 1
    # Cuota definida
    cuotadef = data.loc[data["nolibra"] == nro_libranza, "cuota_definida"].values[0]


    # Calculo de cuotas, amortizacion y generacion de fechas de pago 

    # Fechas de pago (al final de cada mes)
    fechas_pago = pd.date_range(start=fecha_inicio, periods=plazo, freq="M")
    # Cuota fija mensual (fórmula francesa)
    cuota = monto_prestado * (tasa_mensual * (1 + tasa_mensual) ** plazo) / ((1 + tasa_mensual) ** plazo - 1)
    # Saldo inicial
    saldo = monto_prestado
    # Inicializar tabla amortización con fila saldo inicial
    tabla = [{
        "N°": 0,
        "FECHA": "",
        "TASA LIQUIDACION": "",
        "CUOTA": "",
        "SEGURO DEUDORES": round(seguro_deudor),
        "INTERES": "",
        "AMORTIZACION": "",
        "SALDO": round(saldo)
    }]


    # Loop mensual para clcular cuotas, interes, amortizacion y saldo

    for i in range(int(plazo_meses)):
        interes = saldo * tasa_mensual
        amortizacion = cuota - interes

        if i == plazo_meses - 1:
            amortizacion = saldo
            cuota = interes + amortizacion

        saldo -= amortizacion

        fila = {
            "N°": i + 1,
            "FECHA": fechas_pago[i].strftime("%d/%m/%Y"),
            "TASA LIQUIDACION": f"{tasa_mensual * 100:.5f}%",
            "CUOTA": round(cuota),
            "SEGURO DEUDORES": round(seguro_deudor),
            "INTERES": round(interes),
            "AMORTIZACION": round(amortizacion),
            "SALDO": round(max(saldo, 0))
        }
        tabla.append(fila)
    # Convertir a DataFrame
    df = pd.DataFrame(tabla)
    # Convertir FECHA a datetime para merge posterior
    df["FECHA"] = pd.to_datetime(df["FECHA"], format="%d/%m/%Y", errors="coerce")


    # Carga tasa de usura y crear rango de fechas

    df_tasa_usura["Fecha Inicio"] = pd.to_datetime(df_tasa_usura["Fecha Inicio"])
    df_tasa_usura["Fecha Fin"] = pd.to_datetime(df_tasa_usura["Fecha Fin"])
    # Expandir rangos de fechas para usura (un día por fila)
    rangos_usura = []
    for _, fila in df_tasa_usura.iterrows():
        fechas = pd.date_range(start=fila["Fecha Inicio"], end=fila["Fecha Fin"], freq="D")
        for fecha in fechas:
            rangos_usura.append({"FECHA": fecha, "TASA_USURA": fila["TASA_USURA"]})
    df_rangos_usura = pd.DataFrame(rangos_usura)


    # Unir con tasa de usura y reordenar columnas

    df = df.merge(df_rangos_usura, on="FECHA", how="left")
    df["TASA_USURA"] = df["TASA_USURA"].fillna(0)
    # Reordenar para que tasa_usura quede justo después de tasa_liquidacion
    columnas = df.columns.tolist()
    posicion = columnas.index("TASA LIQUIDACION") + 1
    columnas.insert(posicion, columnas.pop(columnas.index("TASA_USURA")))
    df = df[columnas]


    # Armar nueva tabla de automatizacion

    df_ajustado = df.copy()
    # Convertir tasa_liquidacion a decimal
    df_ajustado["TASA_LIQUIDACION_VALOR"] = (
        pd.to_numeric(
            df_ajustado["TASA LIQUIDACION"].str.replace("%", "", regex=False).str.replace(",", "."),
            errors="coerce"
        ) / 100
    )
    # Calcular tasa aplicada (mínimo entre tasa_liq y tasa_usura si existe)
    df_ajustado["TASA APLICADA"] = df_ajustado.apply(
        lambda fila: fila["TASA_LIQUIDACION_VALOR"]
        if pd.isna(fila.get("TASA_USURA", float("nan"))) or fila.get("TASA_USURA", 0) == 0
        else min(fila["TASA_LIQUIDACION_VALOR"], fila["TASA_USURA"]),
        axis=1
    )
    # Obtener cuota de fila 1 para usar después
    cuota = df_ajustado.loc[1, "CUOTA"] if 1 in df_ajustado.index and "CUOTA" in df_ajustado.columns else 0
    # Variables iniciales
    saldo = monto_prestado
    acumulado_interes = 0
    def formatear(numero, decimales=0):
        if pd.isna(numero):
            return ""
        return f"{numero:,.{decimales}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    filas = []
    filas.append({
        "Nº": 0,
        "FECHA": "",
        "TASA_LIQUIDACIÓN": "",
        "TASA DE USURA": "",
        "SEGURO DEUDORES": "",
        "FINANCIACIÓN CAPITAL": "",
        "AMORTIZACIÓN CAPITAL": "",
        "SALDO CAPITAL": formatear(saldo, 0),
        "Acumulado_financiacion_capital": ""
    })
    # Paso 7: recorrer el plazo completo
    for i in range(1, round(plazo) + 1):
        f = df_ajustado.iloc[i]
        tasa_liq = f["TASA_LIQUIDACION_VALOR"]
        tasa_usura = f.get("TASA_USURA", float("nan"))
        tasa_aplicada = f["TASA APLICADA"]
        interes = saldo * tasa_aplicada
        amort = cuota - interes
        saldo -= amort
        acumulado_interes += interes
        # Manejo de fecha
        fecha_str = ""
        if "FECHA" in f and pd.notna(f["FECHA"]):
            try:
                fecha_str = pd.to_datetime(f["FECHA"]).strftime("%d/%m/%Y")
            except Exception:
                fecha_str = str(f["FECHA"])
        filas.append({
            "Nº": int(f["N°"]) if "N°" in f and pd.notna(f["N°"]) else i,
            "FECHA": fecha_str,
            "TASA_LIQUIDACIÓN": formatear(tasa_liq * 100, 5) + "%" if pd.notna(tasa_liq) else "",
            "TASA DE USURA": "" if pd.isna(tasa_usura) or tasa_usura == 0 else formatear(tasa_usura * 100, 6) + "%",
            "SEGURO DEUDORES": formatear(f.get("SEGURO DEUDORES", 0), 0),
            "FINANCIACIÓN CAPITAL": formatear(interes, 0),
            "AMORTIZACIÓN CAPITAL": formatear(amort, 0),
            "SALDO CAPITAL": formatear(saldo, 0),
            "Acumulado_financiacion_capital": formatear(acumulado_interes, 0)
        })
    # Paso 8: construir el DataFrame final
    df_final = pd.DataFrame(filas)[[
        "Nº", "FECHA", "TASA_LIQUIDACIÓN", "TASA DE USURA",
        "SEGURO DEUDORES", "FINANCIACIÓN CAPITAL",
        "AMORTIZACIÓN CAPITAL", "SALDO CAPITAL", "Acumulado_financiacion_capital"
    ]]

    # Calcular cuotas pagas

    
    ### CALCULAR CUOTAS PAGAS ###

    # Asegúrate que fecha_data es un datetime (no una serie)
    fecha_data = pd.to_datetime(data.loc[data["nolibra"] == nro_libranza, "fecha_data"].values[0])
    # Calcular diferencia en meses entre fecha_data y corte
    diff = relativedelta(corte_saldo_sistema, fecha_data)
    meses_adicionales = diff.years * 12 + diff.months
    # Cuotas pagadas = las que ya se habían pagado a la fecha_data + las adicionales desde entonces
    cuotas_pagas_base = plazo - (saldo_cierre_mes_0 / cuotadef)
    cuotas_pagas = cuotas_pagas_base + meses_adicionales


    # Calcular saldo actual

    # Con esta linea evitar que hayan errores en los siguientes pasos
    df_final["Nº"] = pd.to_numeric(df_final["Nº"], errors="coerce")  
    cuotas_causadas = int(cuotas_causadas)
    cuotas_pagas = int(cuotas_pagas)

    # Paso 2: Obtener tasas en formato string desde df_final
    tasa_usura_str = df_final.loc[df_final["Nº"] == cuotas_pagas, "TASA DE USURA"].values[0]
    tasa_liq_str = df_final.loc[df_final["Nº"] == cuotas_pagas, "TASA_LIQUIDACIÓN"].values[0]

    # Paso 3: Limpiar y convertir a float
    tasa_usura = float(tasa_usura_str.replace('%', '').replace(',', '.'))
    tasa_liq = float(tasa_liq_str.replace('%', '').replace(',', '.'))

    # Paso 4: Elegir tasa mínima y pasar a decimal
    tasa = min(tasa_usura, tasa_liq) / 100
    print(f"Tasa aplicada: {tasa:.6f}")

    # Paso 5: Extraer saldo capital y convertir a float
    saldo_capital_str = df_final.loc[df_final["Nº"] == cuotas_pagas, "SALDO CAPITAL"].values[0]
    saldo_capital = float(str(saldo_capital_str).replace('.', '').replace(',', '.'))
    print(f"Saldo capital base: {saldo_capital}")

    # Paso 6: Calcular subtotal obligación mensual por intereses
    dias_mes = calendar.monthrange(datetime.today().year, datetime.today().month)[1]
    subtotal_obligacion_mensual = (saldo_capital * tasa) / dias_mes

    # Paso 7: Calcular días restantes del mes y subtotal obligación proporcional
    dias_restantes = dias_mes - datetime.today().day
    subtotal_obligacion_proporcional = subtotal_obligacion_mensual * dias_restantes

    print(f"Días restantes mes: {dias_restantes}")
    print(f"Subtotal obligación proporcional (intereses prorrateados): {subtotal_obligacion_proporcional:.2f}")

    # Función para extraer acumulados limpiando formato
    def obtener_acumulado(df, numero):
        valor = df.loc[df["Nº"] == numero, "Acumulado_financiacion_capital"].values
        if len(valor) == 0 or str(valor[0]).strip() == '':
            return 0.0
        valor_limpio = str(valor[0]).replace('.', '').replace(',', '.')
        return float(valor_limpio)

    # Paso 8: Comparar cuotas pagas y causadas para calcular ajuste acumulado
    if cuotas_pagas == cuotas_causadas:
        resultado = 0
    elif cuotas_causadas > cuotas_pagas:
        resultado = obtener_acumulado(df_final, cuotas_causadas) - obtener_acumulado(df_final, cuotas_pagas)
    else:
        resultado = obtener_acumulado(df_final, cuotas_pagas) - obtener_acumulado(df_final, cuotas_causadas)
    print(f"Diferencia acumulados (resultado): {resultado:.2f}")

    # Paso 9: Calcular base cálculo para días de liquidación (saldo capital a cuotas causadas)
    fila_base_calculo = cuotas_causadas
    valor_base_calc = df_final.loc[df_final["Nº"] == fila_base_calculo, "SALDO CAPITAL"].values
    base_calculo = float(str(valor_base_calc[0]).replace('.', '').replace(',', '.')) if len(valor_base_calc) else 0.0

    # Paso 10: Calcular días para liquidar y resultado proporcional
    fecha_hoy = datetime.today()
    dias_liquidar = (fecha_hoy - corte_saldo_sistema).days
    if fecha_hoy == corte_saldo_sistema:
        resultado_2 = 0
    else:
        resultado_2 = base_calculo * (tasa / 30) * dias_liquidar
    print(f"Resultado días liquidar (resultado_2): {resultado_2:.2f}")

    # Paso 11: Calcular saldo de seguros proporcional
    saldo_seguros = (cuotas_causadas * seguro_deudor) - (cuotas_pagas * seguro_deudor)
    print(f"Saldo seguros: {saldo_seguros:.2f}")

    # Paso 12: Calcular saldo actual sumando todos los componentes
    saldo_actual = saldo_capital - resultado + resultado_2 + saldo_seguros
    print(f"Saldo actual calculado: {saldo_actual:.2f}")

    resultado = {
        "nro_libranza": nro_libranza,
        "fecha_inicio": fecha_inicio.strftime("%Y-%m-%d"),
        "cuotas_causadas": cuotas_causadas,
        "cuotas_pagas": cuotas_pagas,
        "saldo actual": saldo_actual,
        #"tabla_amortizacion": df_final.to_dict(orient="records")[:40]  # limitar a 40 filas
    }

    return resultado