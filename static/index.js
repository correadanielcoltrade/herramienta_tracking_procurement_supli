document.addEventListener('DOMContentLoaded', () => {
    // Efecto de escritura para el subtítulo
    const subtitle = document.querySelector('.subtitle');
    const originalText = subtitle.textContent;
    subtitle.textContent = '';
    
    let i = 0;
    const typingEffect = setInterval(() => {
        if (i < originalText.length) {
            subtitle.textContent += originalText.charAt(i);
            i++;
        } else {
            clearInterval(typingEffect);
        }
    }, 50);
    
    // Efecto de hover para el botón CTA
    const ctaButton = document.querySelector('.cta-button');
    
    ctaButton.addEventListener('mouseenter', () => {
        ctaButton.style.background = `linear-gradient(135deg, ${getComputedStyle(document.documentElement).getPropertyValue('--accent-color')}, ${getComputedStyle(document.documentElement).getPropertyValue('--primary-color')})`;
    });
    
    ctaButton.addEventListener('mouseleave', () => {
        ctaButton.style.background = `linear-gradient(135deg, ${getComputedStyle(document.documentElement).getPropertyValue('--primary-color')}, ${getComputedStyle(document.documentElement).getPropertyValue('--secondary-color')})`;
    });
    
    // Efecto de scroll suave para todos los enlaces
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            e.preventDefault();
            document.querySelector(this.getAttribute('href')).scrollIntoView({
                behavior: 'smooth'
            });
        });
    });
    
    // Animación de burbujas flotantes
    const floatingShapes = document.querySelector('.floating-shapes');
    for (let i = 0; i < 15; i++) {
        const shape = document.createElement('div');
        shape.classList.add('floating-shape');
        shape.style.left = `${Math.random() * 100}%`;
        shape.style.top = `${Math.random() * 100}%`;
        shape.style.width = `${10 + Math.random() * 20}px`;
        shape.style.height = shape.style.width;
        shape.style.background = `hsl(${Math.random() * 360}, 70%, 70%)`;
        shape.style.opacity = '0.3';
        shape.style.borderRadius = `${Math.random() * 50}%`;
        shape.style.animation = `float ${3 + Math.random() * 7}s infinite ease-in-out ${Math.random() * 5}s`;
        floatingShapes.appendChild(shape);
    }
});