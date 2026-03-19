import os
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, ListItem, ListFlowable
from reportlab.lib.colors import HexColor

def generar_pdf_paso10(output_path):
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

    # Title
    story.append(Paragraph("Documentación Técnica: Pipeline Cuantitativo", title_style))
    story.append(Paragraph("Paso 10: Modelación de Volatilidad Implícita (LR-Brent + JIT)", title_style))
    story.append(Spacer(1, 10))

    # Introducción
    story.append(Paragraph("1. Objetivo Teórico del Módulo", heading_style))
    intro_txt = """
    El Paso 10 (`IVEnricher`) es el núcleo matemático del pipeline. Su misión es descubrir la <b>Volatilidad Implícita (IV)</b> exacta de cada opción transada en el mercado. 
    A diferencia de la volatilidad histórica (que mira al pasado), la IV es la volatilidad que el mercado está "fijando en precio" hacia el futuro. 
    Para hallarla, debemos tomar el precio de mercado de la opción e <i>invertir</i> un modelo de valuación hasta que el precio teórico coincida con el real.
    """
    story.append(Paragraph(intro_txt, body_style))

    # Problema Black-Scholes
    story.append(Paragraph("2. El Problema de Black-Scholes y American Options", heading_style))
    bs_desc = """
    La fórmula cerrada de Black-Scholes-Merton es matemáticamente elegante pero <b>insuficiente</b> para el mercado de acciones moderno estadounidense, debido a que:
    """
    story.append(Paragraph(bs_desc, body_style))
    
    bs_bullets = [
        ListItem(Paragraph("Solo valora opciones Europeas (no ejercitables antes del vencimiento).", bullet_style)),
        ListItem(Paragraph("Asume dividendos cero o los aproxima de forma defectuosa.", bullet_style))
    ]
    story.append(ListFlowable(bs_bullets, bulletType='bullet', start='circle'))

    # Solucion LR
    story.append(Paragraph("3. La Solución: Árbol Binomial de Leisen-Reimer", heading_style))
    lr_desc = """
    En lugar de BS, el sistema construye un <b>Árbol Binomial de 101 pasos</b> mediante el modelo de <b>Leisen-Reimer (LR)</b>. 
    A diferencia del árbol clásico de Cox-Ross-Rubinstein (CRR) que converge oscilando brutalmente (dificultando el root-finding), el árbol LR centra los nodos de precio implícitamente alrededor del Strike.
    """
    story.append(Paragraph(lr_desc, body_style))

    story.append(Paragraph("<i>Mecánica Cuantitativa:</i>", subheading_style))
    lr_bullets = [
        ListItem(Paragraph("En cada nodo (hacia atrás en el tiempo), el modelo compara el valor retenido ('Hold Value') de la opción vs el valor de ejercerla anticipadamente ('Intrinsic Value').", bullet_style)),
        ListItem(Paragraph("Si la opción es Call y el dividendo continuo (`q`) hace que el ejercicio prematuro sea óptimo, el algoritmo registra dicha prima americana.", bullet_style))
    ]
    story.append(ListFlowable(lr_bullets, bulletType='bullet', start='circle'))

    # Root Finding
    story.append(Paragraph("4. Inversión del Precio mediante el Método Brent", heading_style))
    brent_desc = """
    Casi todos los softwares comerciales invierten la volatilidad usando el algoritmo de <b>Newton-Raphson</b>. Este algoritmo funciona usando la sensibilidad del precio a la volatilidad (la Griega <i>Vega</i>). 
    <br/><br/>
    <b>El Problema:</b> Para opciones muy Out-Of-The-Money (OTM) o muy In-The-Money (ITM), la Vega es cercana a 0. Dividir por cero (o casi cero) hace que Newton-Raphson explote y retorne `NaN` o errores de convergencia.
    """
    story.append(Paragraph(brent_desc, body_style))
    
    solution_desc = """
    <b>La Solución Implementada:</b> El Paso 10 reemplaza completamente NR por el <b>Método de Brent (Brent's root-finding algorithm)</b>. 
    Brent combina el método de bisección rígida con la iteración de la secante y la interpolación cuadrática inversa. Aunque toma unas pocas iteraciones más, es matemáticamente robusto y garantiza 100% de convergencia en OTM profundo. El algoritmo atrapa la volatilidad siempre que esté entre el 0.1% y el 2000% (`IV_LO` = 0.001, `IV_HI` = 20.0).
    """
    story.append(Paragraph(solution_desc, body_style))

    # Aceleracion Numba
    story.append(Paragraph("5. Hard-Real-Time Computing: Numba JIT", heading_style))
    numba_desc = """
    Calcular un árbol binomial bidimensional 100 veces por cada iteración del Método de Brent es computacionalmente extremo. Multiplicado por las millones de filas del archivo Parquet, realizar esto en Python puro tardaría horas por cada día.
    <br/><br/>
    Para resolverlo el Paso 10 utiliza <b>Numba LLVM Just-In-Time Compilation</b> (`numba.prange`). Dicho módulo en un archivo externo subyacente (`_numba_iv.py`) transpila el código de cálculo al lenguaje de la máquina (C/C++) durante el primer inicio, derivando la carga vectorial de la grilla de opciones directamente sobre <b>todos los núcleos de la CPU disponibles concurrentemente</b>.
    """
    story.append(Paragraph(numba_desc, body_style))

    # Resultados y Outputs
    story.append(Paragraph("6. Salida Estructurada", heading_style))
    output_text = """
    La nueva variable crucial es añadida a los archivos Parquet bajo los nombres `iv_call` o `iv_put` dependiendo de la iteración. 
    Los archivos son sobreescritos dinámicamente utilizando una metodología "atómica", garantizando integridad de datos incluso si ocurre un corte de energía en el servidor durante la actualización.
    """
    story.append(Paragraph(output_text, body_style))

    # Build PDF
    doc.build(story)
    print(f"PDF generado con éxito en: {output_path}")

if __name__ == "__main__":
    output_pdf = r"c:\proyecto\otros_archivos\Paso_10_Volatilidad_Implicita_LR_Brent.pdf"
    
    # Crear carpeta
    os.makedirs(os.path.dirname(output_pdf), exist_ok=True)
    
    # Generar
    generar_pdf_paso10(output_pdf)
