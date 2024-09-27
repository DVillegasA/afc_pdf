import os
from argparse import ArgumentParser
import jinja2
import pdfkit
import pandas as pd
from datetime import datetime
import calendar
from joblib import Parallel, delayed, cpu_count
from PyPDF2 import PdfReader, PdfWriter

def generate_file(df_main_data, df_cotiz_data, df_user_data, range_start, range_end):
    calendario = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre",
    }

    for i in range(range_start,range_end):
        current_date = datetime.today()

        fecha_full = f"1 de Enero al {calendar.monthrange(current_date.year, current_date.month)[1]} de {calendario[current_date.month]} del {current_date.year}"
        user_name = df_user_data.iloc[0]['nombre']
        user_rut = df_user_data.iloc[0]['rut']
        user_address = f"{df_user_data.iloc[0]['direccion']}, {df_user_data.iloc[0]['comuna']}"
        fecha_inicial = df_main_data.iloc[0]['fecha_inicial'].strftime("%d/%m/%Y")
        saldo_inicial = f"{df_main_data.iloc[0]['saldo_inicial']:,d}"
        total_cotizaciones = f"{df_main_data.iloc[0]['total_cotizaciones']:,d}"
        otros_ingresos = f"{df_main_data.iloc[0]['otros_ingresos']:,d}"
        total_ganancias = f"{df_main_data.iloc[0]['total_ganancias']:,d}"
        total_ingresos = df_main_data.iloc[0]['total_cotizaciones'] + df_main_data.iloc[0]['otros_ingresos'] + df_main_data.iloc[0]['total_ganancias']
        total_comisiones = f"{df_main_data.iloc[0]['total_comisiones']:,d}"
        otros_egresos = f"{df_main_data.iloc[0]['otros_egresos']:,d}"
        uso_cuenta = f"{df_main_data.iloc[0]['uso_cuenta']:,d}"
        total_egresos = df_main_data.iloc[0]['total_comisiones'] + df_main_data.iloc[0]['otros_egresos'] + df_main_data.iloc[0]['uso_cuenta']
        fecha_final = datetime(current_date.year, current_date.month, calendar.monthrange(current_date.year, current_date.month)[1]).strftime("%d/%m/%Y")
        saldo_final = df_main_data.iloc[0]['saldo_inicial'] + total_ingresos - total_egresos
        
        total_ingresos = f"{total_ingresos:,d}"
        total_egresos = f"{total_egresos:,d}"
        saldo_final = f"{saldo_final:,d}"

        razon_social_1 = f"{df_cotiz_data.iloc[0]['razon_social']}" if 1 in df_cotiz_data.index else ''
        mes_pago_1 = f"{calendario[df_cotiz_data.iloc[0]['mes_pago'].month]} - {df_cotiz_data.iloc[0]['mes_pago'].year}" if 1 in df_cotiz_data.index else ''
        total_pago_1 = f"{df_cotiz_data.iloc[0]['total_pago']:,d}" if 1 in df_cotiz_data.index else ''
        razon_social_2 = f"{df_cotiz_data.iloc[1]['razon_social']}" if 2 in df_cotiz_data.index else ''
        mes_pago_2 = f"{calendario[df_cotiz_data.iloc[1]['mes_pago'].month]} - {df_cotiz_data.iloc[1]['mes_pago'].year}" if 2 in df_cotiz_data.index else ''
        total_pago_2 = f"{df_cotiz_data.iloc[1]['total_pago']:,d}" if 2 in df_cotiz_data.index else ''
        razon_social_3 = f"{df_cotiz_data.iloc[2]['razon_social']}" if 3 in df_cotiz_data.index else ''
        mes_pago_3 = f"{calendario[df_cotiz_data.iloc[2]['mes_pago'].month]} - {df_cotiz_data.iloc[2]['mes_pago'].year}" if 3 in df_cotiz_data.index else ''
        total_pago_3 = f"{df_cotiz_data.iloc[2]['total_pago']:,d}" if 3 in df_cotiz_data.index else ''
        razon_social_4 = f"{df_cotiz_data.iloc[3]['razon_social']}" if 4 in df_cotiz_data.index else ''
        mes_pago_4 = f"{calendario[df_cotiz_data.iloc[3]['mes_pago'].month]} - {df_cotiz_data.iloc[3]['mes_pago'].year}" if 4 in df_cotiz_data.index else ''
        total_pago_4 = f"{df_cotiz_data.iloc[3]['total_pago']:,d}" if 4 in df_cotiz_data.index else ''
        razon_social_5 = f"{df_cotiz_data.iloc[4]['razon_social']}" if 5 in df_cotiz_data.index else ''
        mes_pago_5 = f"{calendario[df_cotiz_data.iloc[4]['mes_pago'].month]} - {df_cotiz_data.iloc[4]['mes_pago'].year}" if 5 in df_cotiz_data.index else ''
        total_pago_5 = f"{df_cotiz_data.iloc[4]['total_pago']:,d}" if 5 in df_cotiz_data.index else ''
        razon_social_6 = f"{df_cotiz_data.iloc[5]['razon_social']}" if 6 in df_cotiz_data.index else ''
        mes_pago_6 = f"{calendario[df_cotiz_data.iloc[5]['mes_pago'].month]} - {df_cotiz_data.iloc[5]['mes_pago'].year}" if 6 in df_cotiz_data.index else ''
        total_pago_6 = f"{df_cotiz_data.iloc[5]['total_pago']:,d}" if 6 in df_cotiz_data.index else ''
        razon_social_7 = f"{df_cotiz_data.iloc[6]['razon_social']}" if 7 in df_cotiz_data.index else ''
        mes_pago_7 = f"{calendario[df_cotiz_data.iloc[6]['mes_pago'].month]} - {df_cotiz_data.iloc[6]['mes_pago'].year}" if 7 in df_cotiz_data.index else ''
        total_pago_7 = f"{df_cotiz_data.iloc[6]['total_pago']:,d}" if 7 in df_cotiz_data.index else ''
        razon_social_8 = f"{df_cotiz_data.iloc[7]['razon_social']}" if 8 in df_cotiz_data.index else ''
        mes_pago_8 = f"{calendario[df_cotiz_data.iloc[7]['mes_pago'].month]} - {df_cotiz_data.iloc[7]['mes_pago'].year}" if 8 in df_cotiz_data.index else ''
        total_pago_8 = f"{df_cotiz_data.iloc[7]['total_pago']:,d}" if 8 in df_cotiz_data.index else ''
        razon_social_9 = f"{df_cotiz_data.iloc[8]['razon_social']}" if 9 in df_cotiz_data.index else ''
        mes_pago_9 = f"{calendario[df_cotiz_data.iloc[8]['mes_pago'].month]} - {df_cotiz_data.iloc[8]['mes_pago'].year}" if 9 in df_cotiz_data.index else ''
        total_pago_9 = f"{df_cotiz_data.iloc[8]['total_pago']:,d}" if 9 in df_cotiz_data.index else ''
        razon_social_10 = f"{df_cotiz_data.iloc[9]['razon_social']}" if 10 in df_cotiz_data.index else ''
        mes_pago_10 = f"{calendario[df_cotiz_data.iloc[9]['mes_pago'].month]} - {df_cotiz_data.iloc[9]['mes_pago'].year}" if 10 in df_cotiz_data.index else ''
        total_pago_10 = f"{df_cotiz_data.iloc[9]['total_pago']:,d}" if 10 in df_cotiz_data.index else ''
        razon_social_11 = f"{df_cotiz_data.iloc[10]['razon_social']}" if 11 in df_cotiz_data.index else ''
        mes_pago_11 = f"{calendario[df_cotiz_data.iloc[10]['mes_pago'].month]} - {df_cotiz_data.iloc[10]['mes_pago'].year}" if 11 in df_cotiz_data.index else ''
        total_pago_11 = f"{df_cotiz_data.iloc[10]['total_pago']:,d}" if 11 in df_cotiz_data.index else ''
        razon_social_12 = f"{df_cotiz_data.iloc[11]['razon_social']}" if 12 in df_cotiz_data.index else ''
        mes_pago_12 = f"{calendario[df_cotiz_data.iloc[11]['mes_pago'].month]} - {df_cotiz_data.iloc[11]['mes_pago'].year}" if 12 in df_cotiz_data.index else ''
        total_pago_12 = f"{df_cotiz_data.iloc[11]['total_pago']:,d}" if 12 in df_cotiz_data.index else ''
        total_cotizacion = f"{df_cotiz_data['total_pago'].sum():,d}"

        context = {
            'fecha_full': fecha_full,
            'user_name': user_name,
            'user_rut': user_rut,
            'user_address': user_address,
            'fecha_inicial': fecha_inicial,
            'saldo_inicial': saldo_inicial,
            'total_cotizaciones': total_cotizaciones,
            'otros_ingresos': otros_ingresos,
            'total_ganancias': total_ganancias,
            'total_ingresos': total_ingresos,
            'total_comisiones': total_comisiones,
            'otros_egresos': otros_egresos,
            'uso_cuenta': uso_cuenta,
            'total_egresos': total_egresos,
            'fecha_final': fecha_final,
            'saldo_final': saldo_final,
            'razon_social_1': razon_social_1,
            'mes_pago_1': mes_pago_1,
            'total_pago_1': total_pago_1,
            'razon_social_2': razon_social_2,
            'mes_pago_2': mes_pago_2,
            'total_pago_2': total_pago_2,
            'razon_social_3': razon_social_3,
            'mes_pago_3': mes_pago_3,
            'total_pago_3': total_pago_3,
            'razon_social_4': razon_social_4,
            'mes_pago_4': mes_pago_4,
            'total_pago_4': total_pago_4,
            'razon_social_5': razon_social_5,
            'mes_pago_5': mes_pago_5,
            'total_pago_5': total_pago_5,
            'razon_social_6': razon_social_6,
            'mes_pago_6': mes_pago_6,
            'total_pago_6': total_pago_6,
            'razon_social_7': razon_social_7,
            'mes_pago_7': mes_pago_7,
            'total_pago_7': total_pago_7,
            'razon_social_8': razon_social_8,
            'mes_pago_8': mes_pago_8,
            'total_pago_8': total_pago_8,
            'razon_social_9': razon_social_9,
            'mes_pago_9': mes_pago_9,
            'total_pago_9': total_pago_9,
            'razon_social_10': razon_social_10,
            'mes_pago_10': mes_pago_10,
            'total_pago_10': total_pago_10,
            'razon_social_11': razon_social_11,
            'mes_pago_11': mes_pago_11,
            'total_pago_11': total_pago_11,
            'razon_social_12': razon_social_12,
            'mes_pago_12': mes_pago_12,
            'total_pago_12': total_pago_12,
            'total_cotizacion': total_cotizacion,
        }

        template_loader = jinja2.FileSystemLoader(os.getcwd())
        template_env = jinja2.Environment(loader=template_loader)

        template = template_env.get_template("template.html")
        output_text = template.render(context)

        config = pdfkit.configuration(wkhtmltopdf="/usr/bin/wkhtmltopdf")
        output_file_name = f"test_cvillegas_{i}.pdf"
        output_file_path = os.path.join("output_files", output_file_name)
        pdfkit.from_string(output_text, output_file_path, configuration=config)

        reader = PdfReader(output_file_path)
        writer = PdfWriter()
        password, _ = user_rut = df_user_data.iloc[0]['rut'].split('-')
        password = password.replace('.', '')

        for page in reader.pages:
            writer.add_page(page)

        writer.encrypt(password)

        with open(output_file_path, "wb") as f:
            writer.write(f)

parser = ArgumentParser(description="Demo de generación de PDFs para la AFC.")
parser.add_argument(
    'total_iteration', 
    metavar='N', 
    help="Cantidad de documentos a ser generados. Por defecto genera 100.", 
    type=int
)

args = parser.parse_args()
total_iteration = args.total_iteration

df_main_data = pd.read_csv('afc_bruto.csv', index_col=0)
df_cotiz_data = pd.read_csv('afc_cotiz.csv', index_col=0)
df_user_data = pd.read_csv('afc_user.csv', index_col=0)

df_main_data['fecha_inicial'] = pd.to_datetime(df_main_data['fecha_inicial'], format="%Y%m%d")
df_cotiz_data['mes_pago'] = pd.to_datetime(df_cotiz_data['mes_pago'], format="%Y%m")

rango_paralelo = [(int(i*total_iteration/cpu_count()), int((i+1)*total_iteration/cpu_count())) for i in range(0,cpu_count())]

print(f"Cantidad de documentos a generar: {total_iteration}")

Parallel(n_jobs=-1)(delayed(generate_file)(df_main_data, df_cotiz_data, df_user_data, j[0], j[1]) for j in rango_paralelo)
