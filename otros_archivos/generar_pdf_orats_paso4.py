import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso4.pdf"):
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
    story.append(Paragraph("Análisis de Código run_pipeline", ParagraphStyle(
        name='Sup', fontName='Helvetica-Bold', fontSize=10, textColor=colors.HexColor("#A0AEC0"), alignment=1, spaceAfter=5
    )))
    story.append(Paragraph("Explicación del Paso 4: Universo Global Histórico (Top 50)", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "En el archivo <font name='Courier' color='#D53F8C'>run_pipeline.py</font>, el <b>Paso 4</b> es un titán de procesamiento. "
        "A diferencia del Paso 3 (que miraba solo 5 días), aquí el objetivo es encontrar la '<i>Realeza de Wall Street</i>': aquellos activos "
        "que históricamente han dominado el volumen de opciones desde 2019."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. El Reto: Escanear la Historia Completa",
            "body": "El sistema carga absolutamente todos los archivos '<font name='Courier' color='#D53F8C'>.parquet</font>' "
                    "que residen en tu carpeta Core. Estamos hablando potencialmente de más de 1000 días de mercado. "
                    "Para llevar la cuenta de los tickers ganadores a través de todo ese tiempo, se inicializa un "
                    "<font name='Courier' color='#D53F8C'>collections.Counter()</font>. El contador funciona como un sistema de votos: "
                    "cada vez que un ticker logra entrar al Top 50 en un día específico, recibe +1 voto.",
            "code": "import polars as pl\nimport collections\nimport glob\n\ncore_files = glob.glob(os.path.join(..., '*.parquet'))\nticker_counts = collections.Counter()"
        },
        {
            "title": "B. Tolerancia a Fallos y Nombres Inconsistentes",
            "body": "Dado que estamos leyendo datos del 2019 mezclados con datos del 2025, es altamente probable que el proveedor "
                    "(ORATS) haya cambiado los nombres de las columnas a lo largo de los años. El código hace una inspección microscópica "
                    "del esquema (<font name='Courier' color='#D53F8C'>schema_names</font>) de cada archivo para buscar cómo se llamaron el "
                    "volumen de Call y el volumen de Put ese día en particular ('callVolume' vs 'cVolu' vs 'cVol').",
            "code": "schema = lf.collect_schema().names()\n\ncvol_col = 'callVolume' if 'callVolume' in schema else 'cVolu' if 'cVolu' in schema else ...\npvol_col = 'putVolume' if 'putVolume' in schema else ...\n\n# Asigna dinámicamente o pone Ceros si no existe la columna\nif cvol_col: alias_exprs.append(pl.col(cvol_col).fill_null(0).alias('cVol'))"
        },
        {
            "title": "C. El Torneo Diario (Lazy Aggregation)",
            "body": "Por cada día histórico, Polars utiliza sus promesas perezosas (<font name='Courier' color='#D53F8C'>LazyFrames</font>). "
                    "Suma <font name='Courier' color='#D53F8C'>cVol + pVol</font>, filtra los tickers inválidos que terminan en '_C', los "
                    "ordena desde el trillón de contratos hasta cero de forma descendente, y en esa tabla gigante... le pega un hachazo en "
                    "el puesto 50 (<font name='Courier' color='#D53F8C'>.limit(50)</font>). Finalmente <font name='Courier' color='#D53F8C'>.collect()</font> "
                    "extrae únicamente a la élite ganadora de esa fecha.",
            "code": "top_50_day = (\n    lf.with_columns(alias_exprs)\n    .group_by('ticker')\n    .agg([(pl.col('cVol') + pl.col('pVol')).sum().alias('total_vol')])\n    .filter(~pl.col('ticker').str.contains('_C'))\n    .sort('total_vol', descending=True)\n    .limit(50)\n    .select('ticker')\n    .collect()\n)"
        },
        {
            "title": "D. El Sistema de Votación Diaria",
            "body": "Una vez finalizado el torneo de un día, todos los 50 integrantes obtienen su voto en el contador maestro. Al repetirse "
                    "esto miles de veces, los tickers legendarios como AAPL o TSLA pueden terminar con 1200 votos, mientras que empresas "
                    "efímeras (meme-stocks fugaces) podrían sumar apenas 2 o 3 votos.",
            "code": "ticker_counts.update(top_50_day['ticker'].to_list())"
        },
        {
            "title": "E. El Criterio de Graduación: Superar los 20 Días",
            "body": "Es peligroso depender de 'anomalías' de un solo evento (ej: una empresa pequeña tuvo opciones locas durante dos días "
                    "seguidos por una noticia falsa). El sistema exige que, para ser considerado parte del 'Universo Global Histórico', la "
                    "empresa debió sostenerse dentro del Top 50 norteamericano en **al menos 20 días** distintos a lo largo de toda su historia. "
                    "Los graduados se ordenan alfabéticamente y se guardan como la Biblia del proyecto en el archivo <font name='Courier' color='#D53F8C'>universo.json</font>",
            "code": "lista_final = sorted([\n    ticker for ticker, count in ticker_counts.items() \n    if count >= 20\n])\n\nwith open(universo_path, 'w') as fh:\n    json.dump(lista_final, fh)"
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
