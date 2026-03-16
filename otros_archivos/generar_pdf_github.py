import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.platypus.flowables import KeepTogether

def create_pdf(filename="guia_conexion_github.pdf"):
    # Configurar el documento con márgenes
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        rightMargin=1 * inch,
        leftMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch
    )

    # Estilos
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        name='MainTitle',
        parent=styles['Title'],
        fontName='Helvetica-Bold',
        fontSize=24,
        leading=28,
        textColor=colors.HexColor("#2A4365"), # Azul oscuro profesional
        spaceAfter=20,
        alignment=1 # Centrado
    )

    intro_style = ParagraphStyle(
        name='Intro',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=18,
        textColor=colors.HexColor("#4A5568"), # Gris texto
        spaceAfter=20
    )

    step_title_style = ParagraphStyle(
        name='StepTitle',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#2B6CB0"), # Azul vibrante
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
        textColor=colors.HexColor("#E2E8F0"), # Texto claro
        backColor=colors.HexColor("#1A202C"), # Fondo oscuro
        borderPadding=(8, 10, 8, 10),
        borderWidth=1,
        borderColor=colors.HexColor("#2D3748"),
        borderRadius=5,
        spaceBefore=5,
        spaceAfter=15
    )

    story = []

    # Encabezado
    story.append(Paragraph("Guía Profesional de Python", ParagraphStyle(
        name='Sup', fontName='Helvetica-Bold', fontSize=10, textColor=colors.HexColor("#A0AEC0"), alignment=1, spaceAfter=5
    )))
    story.append(Paragraph("Conexión del Proyecto a GitHub", title_style))
    story.append(Spacer(1, 0.2 * inch))

    # Introducción
    intro_text = (
        "El control de versiones es esencial en cualquier proyecto de software moderno. "
        "Esta guía detalla los pasos para inicializar un repositorio local con <b>Git</b> y "
        "vincularlo a un repositorio remoto en <b>GitHub</b> usando la herramienta <b>GitHub CLI (gh)</b>."
    )
    story.append(Paragraph(intro_text, intro_style))

    # Pasos
    steps_data = [
        {
            "title": "1. Instalar Herramientas Necesarias",
            "body": "Primero, asegúrate de tener instalados tanto Git como GitHub CLI. Puedes descargarlos de forma manual o usar el gestor de paquetes de Windows (winget):",
            "code": "winget install --id Git.Git -e --source winget\nwinget install --id GitHub.cli -e --source winget"
        },
        {
            "title": "2. Crear el archivo .gitignore",
            "body": "Antes de empezar con Git, es fundamental crear un archivo <font name='Courier' color='#D53F8C'>.gitignore</font> en la raíz de tu proyecto para evitar que se suban archivos innecesarios como el directorio <font name='Courier' color='#D53F8C'>venv/</font> o la caché de Python.",
            "code": "echo venv/ >> .gitignore\necho __pycache__/ >> .gitignore"
        },
        {
            "title": "3. Inicializar el Repositorio Local",
            "body": "Navega a la carpeta de tu proyecto y ejecuta los siguientes comandos para crear un repositorio Git, agregar todos los archivos (excepto los ignorados) y crear el commit inicial.",
            "code": "git init\ngit add .\ngit commit -m \"Initial commit\""
        },
        {
            "title": "4. Autenticarse en GitHub CLI",
            "body": "Para poder crear el repositorio en tu cuenta desde la terminal, necesitas iniciar sesión. Sigue las instrucciones del comando y verifica la sesión a través de tu navegador.",
            "code": "gh auth login"
        },
        {
            "title": "5. Crear y Subir el Repositorio Remote",
            "body": "Usaremos GitHub CLI para crear el repositorio en la nube, configurarlo como la ubicación remota (<font name='Courier' color='#D53F8C'>origin</font>) y subir tu código en un solo paso.",
            "code": "gh repo create nombre-de-tu-proyecto --source=. --remote=origin --push --public"
        }
    ]

    for step in steps_data:
        step_flowables = [
            Paragraph(step["title"], step_title_style),
            Paragraph(step["body"], body_style)
        ]
        
        # Procesar los saltos de línea en el bloque de código
        code_text = step["code"].replace('\n', '<br/>')
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
    story.append(Paragraph("Generado de forma automatizada. Guía de GitHub CLI.", footer_style))

    # Construir el PDF
    doc.build(story)
    print(f"PDF generado: {os.path.abspath(filename)}")

if __name__ == "__main__":
    create_pdf()
