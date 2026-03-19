import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso8.pdf"):
    # 1. Configurar el lienzo base
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        rightMargin=1 * inch,
        leftMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch
    )

    # 2. Hojas de Estilos
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        name='MainTitle',
        parent=styles['Title'],
        fontName='Helvetica-Bold',
        fontSize=24,
        leading=28,
        textColor=colors.HexColor("#2A4365"),
        spaceAfter=20,
        alignment=1
    )

    intro_style = ParagraphStyle(
        name='Intro',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=18,
        textColor=colors.HexColor("#4A5568"),
        spaceAfter=20
    )

    step_title_style = ParagraphStyle(
        name='StepTitle',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#2B6CB0"),
        spaceBefore=15,
        spaceAfter=8
    )

    body_style = ParagraphStyle(
        name='Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=11,
        leading=16,
        textColor=colors.HexColor("#2D3748"),
        spaceAfter=8
    )

    code_style = ParagraphStyle(
        name='CodeStyle',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=10,
        leading=14,
        textColor=colors.HexColor("#E2E8F0"),
        backColor=colors.HexColor("#1A202C"),
        borderPadding=(8, 10, 8, 10),
        borderWidth=1,
        borderColor=colors.HexColor("#2D3748"),
        borderRadius=5,
        spaceBefore=5,
        spaceAfter=15
    )

    story = []

    # 3. Encabezado y Títulos
    story.append(Paragraph("Análisis de Código OratsDataManager", ParagraphStyle(
        name='Sup', fontName='Helvetica-Bold', fontSize=10, textColor=colors.HexColor("#A0AEC0"), alignment=1, spaceAfter=5
    )))
    story.append(Paragraph("Explicación del Paso 8: Enriquecimiento de Fechas de Earnings", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "El <b>Paso 8</b> es responsable de inyectar contexto corporativo a las opciones financieras. Un reporte "
        "de ganancias trimestrales (Earnings) produce saltos catastróficos o explosivos en el precio de una acción. Las "
        "opciones que vencen justo después de esa fecha son radicalmente más valiosas (por la alta volatilidad implícita). "
        "Aquí es donde el sistema marca cada opción con el calendario de su empresa para saber en qué momento se encontraba."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. Lectura de la Base Maestra de Earnings",
            "body": "Primero, el algoritmo carga la base de datos descargada en el Paso 5 (<font name='Courier' color='#D53F8C'>universe_earnings.parquet</font>) "
                    "que contiene todos los eventos históricos y futuros de ganancias de nuestro Universo de tickers elegidos.",
            "code": "df_earn = pl.read_parquet(earnings_path)\n# Asegura ordenar por la fecha del reporte para el join\ndf_earn = df_earn.sort('reportDate')"
        },
        {
            "title": "B. Salto de Archivos Ya Enriquecidos (Incrementalidad)",
            "body": "Como siempre, el algoritmo evita reprocesar años de historia inútilmente. Toma el esquema del archivo de la rueda "
                    "(su estructura básica) y verifica si ya se encuentra la columna <font name='Courier' color='#D53F8C'>next_earning_date</font>. "
                    "Si ya existe, se salta íntegramente la lectura del archivo pesado.",
            "code": "schema = pl.scan_parquet(path).collect_schema()\nif 'next_earning_date' in schema.names:\n    return # Saltar para no gastar tiempo"
        },
        {
            "title": "C. El 'Join As-Of' de Polars (Vínculo Temporal Asimétrico)",
            "body": "El clásico cruce en bases de datos (SQL JOIN) exige igualdad exacta, pero aquí cruzamos una fecha de mercado (<font name='Courier' color='#D53F8C'>tradeDate</font>) "
                    "con una fecha esparcida de ganancias. Se utiliza la formidable función <font name='Courier' color='#D53F8C'>join_asof</font> de Polars, la cual encuentra la "
                    "'fecha más cercana hacia atrás' o la 'fecha más cercana hacia adelante' de forma algorítmica sin colgar la computadora.",
            "code": "df_rueda = df_rueda.sort('tradeDate')\n\n# Match hacia ADELANTE (Forward)\n# \"Dame el próximo día de ganancias que viene\"\nfuture_earn = df_rueda.join_asof(\n    df_earn,\n    left_on='tradeDate',\n    right_on='reportDate',\n    by='ticker',\n    strategy='forward'\n)"
        },
        {
            "title": "D. La Inyección Doble (Días previos y futuros)",
            "body": "El sistema ejecuta el <font name='Courier' color='#D53F8C'>join_asof</font> dos veces. La primera en modo <font name='Courier' color='#D53F8C'>backward</font> "
                    "para encontrar cuál fue el último reporte de Earnings (<font name='Courier' color='#D53F8C'>prev_earning_date</font>) y la segunda en modo <font name='Courier' color='#D53F8C'>forward</font> "
                    "para saber la inminente fecha esperada del próximo reporte (<font name='Courier' color='#D53F8C'>next_earning_date</font>).",
            "code": "df_rueda = df_rueda.with_columns([\n    past_earn['reportDate'].alias('prev_earning_date'),\n    future_earn['reportDate'].alias('next_earning_date')\n])"
        },
        {
            "title": "E. Sobreescritura Final del Parquet",
            "body": "Habiendo adherido permanentemente estas dos columnas a cada fila, reemplaza el viejo archivo <font name='Courier' color='#D53F8C'>.parquet</font> "
                    "en su ubicación (dentro de <font name='Courier' color='#D53F8C'>ruedas_call_earn</font> o <font name='Courier' color='#D53F8C'>ruedas_put_earn</font>). "
                    "Estos datos se volverán imprescindibles para cálculos como 'Clean IV' (extirpación de varianza del evento).",
            "code": "tmp_path = path + '.tmp'\ndf_rueda.write_parquet(tmp_path, compression='snappy')\nos.remove(path)\nos.rename(tmp_path, path)"
        }
    ]

    for step in steps_data:
        step_flowables = [
            Paragraph(step["title"], step_title_style),
            Paragraph(step["body"], body_style)
        ]
        
        code_text = step["code"].replace('\n', '<br/>').replace(' ', '&nbsp;')
        step_flowables.append(Paragraph(code_text, code_style))
        
        story.append(KeepTogether(step_flowables))
        story.append(Spacer(1, 10))

    # Pie de página
    story.append(Spacer(1, 30))
    footer_style = ParagraphStyle(
        name='Footer',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=colors.HexColor("#A0AEC0"),
        alignment=1
    )
    story.append(Paragraph("Reporte Modular: Explicación de Código - Generado de forma automatizada", footer_style))

    doc.build(story)
    print(f"PDF generado exitosamente: {os.path.abspath(filename)}")

if __name__ == "__main__":
    create_pdf()
