import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="explicacion_orats_paso3_5.pdf"):
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
    story.append(Paragraph("Explicación del Paso 3.5 / 4: Universo Global Histórico", title_style))
    story.append(Spacer(1, 0.2 * inch))

    intro_text = (
        "El <b>Paso 3.5 (formalmente Paso 4 en el código)</b> de tu script <font name='Courier' color='#D53F8C'>run_pipeline.py</font> tiene un objetivo clave: "
        "en lugar de mirar el volumen de un solo día o de forma estática, escanea <b>toda la historia</b> de los datos Core para extraer un Universo robusto "
        "y sobreviviente: aquellas acciones que han estado consistentemente en el <b>Top 50</b> de mayor volumen negociado de opciones."
    )
    story.append(Paragraph(intro_text, intro_style))

    # 4. Datos Estructurados de la Explicación
    steps_data = [
        {
            "title": "A. Preparación y Escaneo Masivo (Lectura de Historial)",
            "body": "El sistema utiliza <font name='Courier' color='#D53F8C'>glob</font> para enlistar todos los miles de archivos parquet históricos en "
                    "<font name='Courier' color='#D53F8C'>datos_cores</font>. Para llevar el registro de qué acciones logran entrar al Top, crea un "
                    "<font name='Courier' color='#D53F8C'>collections.Counter()</font>. Un Counter es ideal porque nos permite ir sumándole 'días de victoria' a cada ticker muy fácilmente.",
            "code": "core_files = glob.glob(os.path.join(BASE_DATA_DIR, '*.parquet'))\nticker_counts = collections.Counter()\nfor f in core_files:\n    lf = pl.scan_parquet(f) # Carga perezosa con Polars"
        },
        {
            "title": "B. Suma de Volumen (Calls + Puts)",
            "body": "Dentro del bucle, para cada día (cada archivo), identifica las columnas del volumen. Si estas columnas (cVolu / pVolu) han tenido variaciones de nombre "
                    "en la historia según los caprichos de la API, usa condicionales para detectarlas. Luego, crea una expresión de <b>Polars</b> para sumar el volumen total operado: "
                    "<font name='Courier' color='#D53F8C'>total_vol = cVol + pVol</font>.",
            "code": "alias_exprs.append(pl.col(cvol_col).fill_null(0).alias('cVol'))\n# ...\n.agg([\n    (pl.col('cVol') + pl.col('pVol')).sum().alias('total_vol')\n])"
        },
        {
            "title": "C. Filtro de Derivados y Ranking Diario (Top 50)",
            "body": "Con el volumen sumado por ticker, elimina los activos que contienen <font name='Courier' color='#D53F8C'>_C</font> (sufijos que denotan warrants o clases de acciones raras). "
                    "Inmediatamente después, ordena el DataFrame de mayor a menor volumen (<font name='Courier' color='#D53F8C'>descending=True</font>) y corta la lista en los primeros 50 puestos (<font name='Courier' color='#D53F8C'>limit(50)</font>).",
            "code": ".filter(~pl.col('ticker').str.contains('_C'))\n.sort('total_vol', descending=True)\n.limit(50)\n.select('ticker')\n.collect()"
        },
        {
            "title": "D. El Sistema de Votación Acumulativa (Update Counter)",
            "body": "Cada vez que Polars devuelve la lista de los 50 tickers ganadores de un día específico, el código 'actualiza' el <font name='Courier' color='#D53F8C'>Counter</font>. "
                    "Esto significa que si TSLA salió en el Top 50 del lunes y el martes, su puntaje sube a 2. Si un ticker fue muy operado un solo día por una noticia, solo recibe 1 punto.",
            "code": "ticker_counts.update(top_50_day['ticker'].to_list())"
        },
        {
            "title": "E. Criterio de Supervivencia (Mínimo 20 Días)",
            "body": "Finalmente, habiendo escaneado todos los años de historia, el algoritmo descarta el ruido. Selecciona estrictamente aquellos "
                    "tickers que acumularon <b>al menos 20 días (puntos)</b> dentro del Top 50. Esto garantiza que nuestro Universo Global Histórico "
                    "esté compuesto por empresas con liquidez comprobada y prolongada, no solo anomalías pasajeras. La lista final se guarda en <font name='Courier' color='#D53F8C'>universo.json</font>.",
            "code": "lista_final = sorted([\n    ticker for ticker, count in ticker_counts.items() \n    if count >= 20\n])\n\nwith open(universo_path, 'w') as fh:\n    json.dump(lista_final, fh, indent=4)"
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
