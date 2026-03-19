import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso3.pdf"):
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
    story.append(Paragraph("Explicación del Paso 3: Universo Global (Top 2000)", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "El <b>Paso 3</b> de tu script localiza los activos financieros (tickers) más relevantes y líquidos del mercado. "
        "En lugar de descargar y procesar datos costosos para más de 10,000 acciones que existen en EE.UU., el método "
        "<font name='Courier' color='#D53F8C'>get_universe()</font> selecciona inteligentemente a los "
        "mejores contendientes basándose en el volumen real de operaciones con opciones de los últimos 5 días."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. ¿De dónde saca la muestra? (Lookback Period)",
            "body": "No escanea los 5 años de historia porque los mercados cambian. Una empresa muy líquida en 2020 puede estar "
                    "muerta hoy. Por ello, el script usa el parámetro <font name='Courier' color='#D53F8C'>lookback_days=5</font>. "
                    "Clasifica todos tus archivos <font name='Courier' color='#D53F8C'>.parquet</font> descargados por orden alfabético (que al ser fechas "
                    "AÑO-MES-DIA equivale a un orden cronológico perfecto) y toma solamente los 5 más recientes.",
            "code": "files = glob.glob(os.path.join(self.data_dir, '*.parquet'))\nfiles.sort() # Orden cronológico natural\nrecent_files = files[-lookback_days:]"
        },
        {
            "title": "B. Lectura Perezosa Ultra-Rápida (Polars LazyFrames)",
            "body": "Para leer los Parquets, usa <font name='Courier' color='#D53F8C'>pl.scan_parquet(f)</font> en lugar de <font name='Courier' color='#D53F8C'>read_parquet</font>. "
                    "La diferencia es monumental: 'scan' no carga el archivo a la Memoria RAM. En su lugar, Polars crea un 'mapa de intenciones' "
                    "y promete ejecutar los cálculos más tarde. Además, normaliza los nombres de las columnas en caso de que ORATS los haya cambiado a mitad de camino.",
            "code": "lfs = []\nfor f in recent_files:\n    lf = pl.scan_parquet(f)\n    # ... (normalización de columnas price/volume) ...\n    lf = lf.select(['ticker', 'stkPx', 'cVolu', 'pVolu'])\n    lfs.append(lf)"
        },
        {
            "title": "C. Limpieza y Filtros de Calidad",
            "body": "Existen cientos de activos basura o extraños (Warrants, Unidades, Opciones de Penny Stocks). El código implementa un filtro "
                    "triple garantizando que solo pasen acciones operables:<br/>"
                    "1. Que el precio de la acción sea mayor a $10.0 (<font name='Courier' color='#D53F8C'>stkPx > 10.0</font>).<br/>"
                    "2. Que el Ticker no esté vacío (nulo).<br/>"
                    "3. Que el Ticker no contenga guiones bajos (suelen ser derivados exóticos tipo SPX_W).",
            "code": "filtered = combined.filter(\n    (pl.col('stkPx') > 10.0) & \n    (pl.col('ticker').is_not_null()) &\n    (~pl.col('ticker').str.contains('_'))\n)"
        },
        {
            "title": "D. Agrupación y Ranking Final (El Top 2000)",
            "body": "Con la tabla combinada y filtrada, el sistema agrupa todas las filas por el nombre del Ticker. Suma todo el volumen "
                    "de compras (Calls) y ventas (Puts) de la semana, y calcula el precio promedio de la acción. "
                    "Luego simplemente ordena la tabla desde el volumen semanal más gigante hasta el más pequeño y se queda de forma estricta "
                    "sólo con los primeros 2,000.",
            "code": "agg = filtered.group_by('ticker').agg([\n    (pl.col('cVolu').sum() + pl.col('pVolu').sum()).alias('total_vol_period'),\n    pl.col('stkPx').mean().alias('avg_price')\n])\n\ntop_tickers = agg.sort('total_vol_period', descending=True).limit(top_n)"
        },
        {
            "title": "E. Ejecución Mágica y Guardado (Collect y JSON)",
            "body": "Apenas en este punto se invoca la palabra clave <font name='Courier' color='#D53F8C'>.collect()</font>. Polars despierta "
                    "su motor interno en C++ / Rust, optimiza todo el árbol de consultas matemáticas, ignora columnas irrelevantes en el disco, "
                    "y arroja el resultado en microsegundos.<br/>"
                    "Esta codiciada lista dorada de 2000 símbolos se guarda físicamente en el archivo maestro "
                    "<font name='Courier' color='#D53F8C'>universe_top2000.json</font> para que el resto de tu pipeline (Descargas Live o Backtesting) "
                    "pueda consumirla sin tener que recalcular nada.",
            "code": "universe_list = top_tickers.select('ticker').collect().to_series().to_list()\n\nwith open(uni_path, 'w') as f:\n    json.dump(universe_list, f, indent=4)"
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
