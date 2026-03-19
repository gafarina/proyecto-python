import os
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, ListItem, ListFlowable
from reportlab.lib.colors import HexColor

def generar_pdf_paso11(output_path):
    doc = SimpleDocTemplate(output_path, pagesize=letter,
                            rightMargin=50, leftMargin=50,
                            topMargin=50, bottomMargin=50)

    styles = getSampleStyleSheet()
    
    # Custom Styles
    title_style = ParagraphStyle(
        'TitleStyle',
        parent=styles['Heading1'],
        fontSize=20,
        textColor=HexColor("#1e3a8a"),
        spaceAfter=15,
        alignment=1
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
        alignment=1,
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

    # Title
    story.append(Paragraph("Documentación Técnica: Pipeline Cuantitativo", title_style))
    story.append(Paragraph("Paso 11: Extirpación de Riesgo de Evento (Clean IV)", title_style))
    story.append(Spacer(1, 10))

    # Introducción
    story.append(Paragraph("1. Objetivo Teórico del Módulo", heading_style))
    intro_txt = """
    El Paso 11 (`CleanIVEnricher`) ejecuta uno de los procedimientos matemáticos más valiosos del sistema: el <b>Earnings Variance Stripping</b>. 
    <br/><br/>
    En la vida real, los reportes de resultados financieros (Earnings) son eventos binarios que inyectan una enorme cantidad de prima (y varianza implícita) de la noche a la mañana. 
    Esto "contamina" la curva de volatilidad. Un algoritmo predictivo o de machine learning se confundiría creyendo que la acción subyacente es naturalmente ultrarríesgosa, cuando en realidad solo está tasando el gap de los earnings. 
    El Paso 11 identifica quirúrgicamente esa varianza "extra" y la extirpa de todas las opciones posteriores.
    """
    story.append(Paragraph(intro_txt, body_style))

    # Business Days vs Calendar Days
    story.append(Paragraph("2. Días Hábiles vs Días Calendario (El Efecto Fin de Semana)", heading_style))
    biz_desc = """
    A diferencia del Paso 10 que calcula la IV usando días calendario (DTE / 365), la <b>extirpación de eventos se realiza operando sobre Días Hábiles Bursátiles (Base 252)</b>.
    """
    story.append(Paragraph(biz_desc, body_style))
    
    biz_bullets = [
        ListItem(Paragraph("¿Por qué? Porque el riesgo del Evento ocurre en 1 día de mercado (overnight). Si usamos días calendario, un fin de semana diluiría artificialmente la volatilidad implícita diaria, arruinando el cálculo del tamaño del salto.", bullet_style)),
        ListItem(Paragraph("El código utiliza `numpy.busday_count` para traducir los DTE a `biz_days` exactos, obviando Sábados y Domingos.", bullet_style))
    ]
    story.append(ListFlowable(biz_bullets, bulletType='bullet', start='circle'))

    # Mecánica Cuantitativa
    story.append(Paragraph("3. Mecánica Cuantitativa del Forward Variance", heading_style))
    
    story.append(Paragraph("El sistema aisla el peso del evento en 3 sub-pasos ejecutados en Numba:", subheading_style))

    math_bullets = [
        ListItem(Paragraph("<b>Determinación de IV Pre y Post:</b> El algoritmo localiza la opción <i>At-The-Money (ATM)</i> que expira exactamente antes (o el mismo día) del evento (t1), y la opción más cercana posterior al evento (t2).", bullet_style)),
        ListItem(Paragraph("<b>Total Variance (TV):</b> La volatilidad (escala de desviación estándar) no se puede sumar. Se debe trabajar en escalas de Varianza. TV = IV² × t.", bullet_style)),
        ListItem(Paragraph("<b>Descubrimiento del Salto:</b> La porción exacta de varianza agregada por los earnings es igual a la Varianza Adelantada (Forward Variance) que abarca ese lapso temporal, menos la Varianza Natural que debió transcurrir en ese mismo lapso si no hubiera existido el evento.", bullet_style))
    ]
    story.append(ListFlowable(math_bullets, bulletType='bullet', start='circle'))

    # Fórmulas
    story.append(Paragraph("<b>4. Ecuaciones Principales</b>", subheading_style))
    
    story.append(Paragraph("1. Forward Variance (TV_post - TV_pre):", subheading_style))
    story.append(Paragraph("FW_TV = [ IV_post² × t_post ] - [ IV_pre² × t_pre ]", formula_style))
    
    story.append(Paragraph("2. Varianza Base o Natural (Asumiendo que IV post debida ser la pre):", subheading_style))
    story.append(Paragraph("TV_natural = IV_pre² × (t_post - t_pre)", formula_style))

    story.append(Paragraph("3. Tamaño del Choque de Earnings (W_evento):", subheading_style))
    story.append(Paragraph("W_evento = FW_TV - TV_natural", formula_style))

    story.append(Paragraph("4. Resta de Evento y Conversión a Volatilidad Limpia:", subheading_style))
    story.append(Paragraph("TV_clean = TV_total - W_evento <br/><br/> IV_clean = √(TV_clean / t) ", formula_style))
    
    # Resumen Funcional
    story.append(Paragraph("5. Hard-Real-Time en Parallel_Numba", heading_style))
    numba_desc = """
    Dado que este algoritmo debe identificar dinámicamente qué contratos están contaminados (agrupándolos por cadena temporal y ticker), 
    su cálculo a lo largo de un Dataframe Pandas/Polars masivo tomaría innumerables minutos. 
    Gracias a la compilación en tiempo de ejecución (JIT) `compute_clean_iv_batch`, todo este modelo de Stripping de Forward Variance se ejecuta en escasos milisegundos por fecha histórica.
    La salida es la columna de oro final <b>`iv_clean`</b>, indispensable para construir la Superficie Anti-Gravedad local en el Paso 11.6.  
    """
    story.append(Paragraph(numba_desc, body_style))

    # Build PDF
    doc.build(story)
    print(f"PDF generado con éxito en: {output_path}")

if __name__ == "__main__":
    output_pdf = r"c:\proyecto\otros_archivos\Paso_11_Clean_IV_y_Varianza.pdf"
    
    # Crear carpeta
    os.makedirs(os.path.dirname(output_pdf), exist_ok=True)
    
    # Generar
    generar_pdf_paso11(output_pdf)
