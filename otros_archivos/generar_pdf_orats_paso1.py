import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso1.pdf"):
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
    story.append(Paragraph("Explicación del Paso 1: Descarga Histórica Incremental", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "El <b>Paso 1</b> de tu script <font name='Courier' color='#D53F8C'>orats_data_manager.py</font> es el corazón del sistema para la "
        "recolección de datos históricos de operaciones de mercado desde la API de ORATS. Su diseño es <i>robusto</i>, <i>asíncrono</i> e <i>incremental</i>."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. ¿Por qué es Incremental? (get_existing_dates)",
            "body": "El sistema no descarga toda la historia cada vez que lo ejecutas. Primero, usa la librería <font name='Courier' color='#D53F8C'>glob</font> "
                    "para revisar qué archivos <font name='Courier' color='#D53F8C'>.parquet</font> ya tienes en tu carpeta <font name='Courier' color='#D53F8C'>datos_cores</font>. "
                    "Extrae las fechas del nombre del archivo y crea un conjunto matemático (Set) de días ya completados. Esto ahorra tiempo y uso de API.",
            "code": "def get_existing_dates(self) -> Set[datetime.date]:\n    files = glob.glob(...\n    # Retorna {2024-01-02, 2024-01-03...}"
        },
        {
            "title": "B. ¿Cómo sabe qué días debía haber mercado? (get_trading_days)",
            "body": "El mercado no opera los fines de semana ni los feriados. Al usar <font name='Courier' color='#D53F8C'>pandas.tseries.holiday</font> "
                    "el código calcula exactamente los días de operación correctos combinando Feriados Federales y Viernes Santos. Recientemente "
                    "también añadimos matemáticamente el luto excepcional por Jimmy Carter para que el script no intente descargar o reportar ese día perdido.",
            "code": "cal = USFederalHolidayCalendar()\n# ... se unen los feriados\nbday_us = CustomBusinessDay(holidays=all_holidays)\nreturn [d.date() for d in dt_range]"
        },
        {
            "title": "C. Concurrencia de Descarga: El Poder de `asyncio` (download_history_async)",
            "body": "Una vez comparados los días que <i>debieran existir</i> contra los que <i>ya tienes en disco</i>, el sistema aparta los 'Días Faltantes'. "
                    "En programación tradicional (síncrona), el código pediría el día 1, esperaría la respuesta del servidor (lo cual incluye la latencia de red "
                    "y el tiempo de procesamiento de ORATS), la guardaría, y <b>recién entonces</b> pediría el día 2. Esto genera un 'cuello de botella de Entrada/Salida' "
                    "(I/O Bound).<br/><br/>"
                    "Para solucionar esto, <font name='Courier' color='#D53F8C'>download_history_async</font> utiliza el modelo asíncrono moderno de Python con las librerías "
                    "<font name='Courier' color='#D53F8C'>asyncio</font> y <font name='Courier' color='#D53F8C'>aiohttp</font>.<br/><br/>"
                    "<b>¿Cómo funciona paso a paso?</b><br/>"
                    "1. Agrupa los días faltantes en bloques (chunks) de 5 días.<br/>"
                    "2. Con <font name='Courier' color='#D53F8C'>aiohttp.ClientSession()</font>, mantiene una única conexión persistente hacia la API de ORATS "
                    "(connection pooling), evitando renegociar las credenciales HTTPS cada milisegundo.<br/>"
                    "3. Prepara 5 peticiones u 'órdenes de trabajo' (Tasks) simultáneas: <font name='Courier' color='#D53F8C'>[self.fetch_day(session, d) for d in chunk]</font>.<br/>"
                    "4. Usa <font name='Courier' color='#D53F8C'>await asyncio.gather(*tasks)</font>. Este comando es mágico: 'aprieta el gatillo' de las 5 peticiones al mismo tiempo. "
                    "Mientras el código espera que el servidor de ORATS devuelva los datos del día 1, Python no se queda congelado; automáticamente cede el control temporal "
                    "para enviar la petición del día 2, luego la del día 3, etc. Cuando las 5 respuestas llegan provenientes de internet, el código retoma el control, "
                    "las empaqueta juntas y finalmente las guarda en disco.<br/><br/>"
                    "Esto reduce el tiempo de descarga a una quinta parte comparado con el código tradicional, respetando los límites de carga (Rate Limits) del proveedor "
                    "gracias al descanso controlado mediante <font name='Courier' color='#D53F8C'>await asyncio.sleep(0.5)</font>.",
            "code": "chunk_size = 5\nasync with aiohttp.ClientSession() as session:\n    for i in range(0, len(missing_days), chunk_size):\n        chunk = missing_days[i : i + chunk_size]\n        # Preparar 5 peticiones\n        tasks = [self.fetch_day(session, d) for d in chunk]\n        # Ejecutarlas TODAS AL MISMO TIEMPO y esperar resultado\n        results = await asyncio.gather(*tasks)\n        \n        # Procesar los 5 resultados...\n        await asyncio.sleep(0.5) # Pausa amigable para la API"
        },
        {
            "title": "D. El Agente Navegador (fetch_day)",
            "body": "Es la función interna encargada del 'diálogo' con ORATS. Toma tu Token y le pregunta cortésmente por una fecha específica. "
                    "Si ORATS responde con un 200 OK y entrega datos, los toma. Si ORATS no envía datos (porque fue un feriado no programado por ejemplo), "
                    "la función es capaz de asimilarlo y retornar valor <font name='Courier' color='#D53F8C'>None</font> sin colgar el programa.",
            "code": "if response.status == 200:\n    data = await response.json()\n    if isinstance(data, dict) and ...\n        return date_str, data['data']"
        },
        {
            "title": "E. Transformación y Guardado Rápido (save_parquet)",
            "body": "Por cada fragmento de datos descargado se genera de inmediato un archivo eficiente en disco. Se usa la ultramoderna "
                    "y veloz librería <b>Polars</b> en lugar de Pandas. Polars carga la información, normaliza rápidamente los nombres sospechosos "
                    "de la API (ej: <font name='Courier' color='#D53F8C'>callVolume</font> lo pasa a <font name='Courier' color='#D53F8C'>cVolu</font>) "
                    "y comprime el resultado usando el algoritmo 'snappy' en formato <font name='Courier' color='#D53F8C'>.parquet</font>.",
            "code": "df = pl.DataFrame(records)\n# ... normalización ...\ndf.write_parquet(out_path, compression='snappy')\nprint(f'[OK] Guardado {date_str}')"
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
