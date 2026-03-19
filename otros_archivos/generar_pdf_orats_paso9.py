import os
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, ListItem, ListFlowable
from reportlab.lib.colors import HexColor

def generar_pdf_paso9(output_path):
    doc = SimpleDocTemplate(output_path, pagesize=letter,
                            rightMargin=50, leftMargin=50,
                            topMargin=50, bottomMargin=50)

    styles = getSampleStyleSheet()
    
    # Estilos Personalizados
    title_style = ParagraphStyle(
        'TitleStyle',
        parent=styles['Heading1'],
        fontSize=20,
        textColor=HexColor("#1e3a8a"),
        spaceAfter=15,
        alignment=1 # Center
    )
    
    heading_style = ParagraphStyle(
        'HeadingStyle',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=HexColor("#2563eb"),
        spaceBefore=15,
        spaceAfter=10
    )
    
    subheading_style = ParagraphStyle(
        'SubHeadingStyle',
        parent=styles['Heading3'],
        fontSize=12,
        textColor=HexColor("#334155"),
        spaceBefore=10,
        spaceAfter=8,
        fontName='Helvetica-Bold'
    )
    
    body_style = ParagraphStyle(
        'BodyStyle',
        parent=styles['Normal'],
        fontSize=11,
        leading=15,
        spaceAfter=15
    )
    
    formula_style = ParagraphStyle(
        'FormulaStyle',
        parent=styles['Normal'],
        fontSize=12,
        leading=16,
        spaceAfter=15,
        alignment=1, # Center
        fontName='Courier-Oblique',
        textColor=HexColor("#0f172a"),
        backColor=HexColor("#f1f5f9"),
        borderPadding=8
    )

    bullet_style = ParagraphStyle(
        'BulletStyle',
        parent=styles['Normal'],
        fontSize=11,
        leading=14,
        spaceAfter=5
    )

    story = []

    # Título Principal
    story.append(Paragraph("Documentación Técnica: Pipeline Cuantitativo", title_style))
    story.append(Paragraph("Paso 9: Enriquecimiento de Dividendos y Curva de Tasas Libres de Riesgo", title_style))
    story.append(Spacer(1, 10))

    # Introducción
    story.append(Paragraph("1. Objetivo Teórico del Módulo", heading_style))
    intro_txt = """
    El Paso 9 (mediante la clase `FMPDataEnricher`) es el puente financiero que prepara los datos crudos del mercado para el cálculo matemático de las Griegas y la Volatilidad Implícita (que ocurrirá en el Paso 10).
    <br/><br/>
    Para que cualquier modelo de simulación de opciones (como Black-Scholes generalizado, árboles binomiales CRR, o Leisen-Reimer) funcione sin arbitraje, es estrictamente necesario que los inputs de <b>Tasa Libre de Riesgo</b> y el <b>Rendimiento de Dividendos</b> estén alineados al horizonte temporal de expiración de la opción (DTE) y que ambos estén expresados en formato de capitalización continua.
    """
    story.append(Paragraph(intro_txt, body_style))

    # Dividendos Continuos
    story.append(Paragraph("2. Dividend Yield Continuo (div_yield_cont)", heading_style))
    div_desc = """
    La herramienta descarga de la API de Financial Modeling Prep el historial de dividendos pagados por el activo. En vez de tomar los dividendos como salidas de caja estáticas (que complican los modelos diferenciales en tiempo continuo), calculamos el <b>Dividend Yield Trailing Twelve Months (TTM) continuo</b>.
    """
    story.append(Paragraph(div_desc, body_style))
    
    story.append(Paragraph("<b>Metodología:</b>", subheading_style))
    div_bullets = [
        ListItem(Paragraph("Suma de todos los dividendos ajustados entregados en los últimos 365 días previos a la fecha de la transacción (tradeDate).", bullet_style)),
        ListItem(Paragraph("Cálculo del Yield dividiendo la suma entre el precio spot actual (`stockPrice`).", bullet_style)),
        ListItem(Paragraph("Conversión a capitalización continua mediante logaritmo natural.", bullet_style))
    ]
    story.append(ListFlowable(div_bullets, bulletType='bullet', start='circle'))
    
    story.append(Paragraph("<b>Fórmula Matemática:</b>", subheading_style))
    story.append(Paragraph("q = ln( 1 + [ Σ adjDividend(TTM) / stockPrice ] )", formula_style))

    # Curva Estructurada de Tasas Libres de Riesgo
    story.append(Paragraph("3. Tasa Libre de Riesgo Continua (risk_free_rate)", heading_style))
    rf_desc = """
    Asumir una tasa plana del 5% para todas las opciones es un error en mesas de trading institucionales. El valor temporal del dinero tiene una <i>Estructura Temporal</i> (Yield Curve). El sistema extrae de FMP la curva de los Bonos del Tesoro estadounidense par la fecha exacta de transacción, conformada por nodos desde 1 mes hasta 30 años.
    """
    story.append(Paragraph(rf_desc, body_style))

    story.append(Paragraph("<b>3.1. Equivalencia de Tasas (BEY a APY a R_c)</b>", subheading_style))
    rf_teoria = """
    Las tasas que publica el tesoro (US Treasury) vienen en formato <b>BEY (Bond Equivalent Yield)</b>, asumiendo capitalización semestral. Los modelos de opciones europeos y americanos requieren tasas de <b>capitalización continua</b>.
    """
    story.append(Paragraph(rf_teoria, body_style))

    story.append(Paragraph("Paso 1: Conversión a Tasa Efectiva Anual (APY):", subheading_style))
    story.append(Paragraph("APY = (1 + BEY / 2)² - 1", formula_style))
    
    story.append(Paragraph("Paso 2: Conversión a Tasa Continua (r_c):", subheading_style))
    story.append(Paragraph("r_c = ln(1 + APY)", formula_style))

    story.append(Paragraph("<b>3.2. Interpolación Avanzada (Clamped Cubic Spline)</b>", subheading_style))
    spline_desc = """
    Como los nodos del tesoro son fijos (ej: 30 días o 91 días), una opción que vence en exactamente 45 días (DTE = 45) necesita una tasa interpolada.  
    En vez de interpolación lineal (que genera "codos" que distorsionan la Griega Rho), el sistema utiliza un <b>Spline Cúbico Restringido (Clamped Cubic Spline)</b> de Scipy.
    <br/><br/>
    Esto asegura la derivabilidad (curva suave sin quiebres) modelando matemáticamente los tramos de la curva de rendimientos y garantizando que las condiciones de borde no oscilen salvajemente (fenómeno de Runge). La tasa finalmente calculada se le asigna paramétricamente a la opción con la convención <b>Actual/365</b>.
    """
    story.append(Paragraph(spline_desc, body_style))
    
    # Consideraciones de Velocidad 
    story.append(Paragraph("4. Caché e Incrementalidad Extrema", heading_style))
    cache_desc = """
    Si bien los cálculos son matemáticamente pesados y requieren millones de peticiones API teóricas, el sistema guarda una <b>instantánea caché (JSON local) de la curva del tesoro para cada día de la historia.</b> Si múltiples acciones transan opciones un Martes, la curva del Tesoro del Martes se baja una vez y se lee de disco en milisegundos para las millones de iteraciones subsecuentes de spline interpolation.
    """
    story.append(Paragraph(cache_desc, body_style))

    # Construir PDF
    doc.build(story)
    print(f"PDF generado con éxito en: {output_path}")

if __name__ == "__main__":
    output_pdf = r"c:\proyecto\otros_archivos\Paso_9_Teoria_Financiera_Dividendos_y_Tasas.pdf"
    
    # Crear carpeta si no existe
    os.makedirs(os.path.dirname(output_pdf), exist_ok=True)
    
    # Ejecutar generación
    generar_pdf_paso9(output_pdf)
