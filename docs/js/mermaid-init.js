document.addEventListener('DOMContentLoaded', function() {
  if (typeof mermaid !== 'undefined') {
    // Initialize mermaid with config
    mermaid.initialize({ 
      startOnLoad: false,
      theme: 'default',
      securityLevel: 'loose'
    });
    
    // Find all code blocks and check if they contain mermaid code
    document.querySelectorAll('div.highlight pre').forEach(function(pre) {
      const code = pre.textContent;
      
      // Check if this block starts with a mermaid diagram keyword
      if (code.trim().startsWith('graph') || 
          code.trim().startsWith('sequenceDiagram') ||
          code.trim().startsWith('classDiagram') ||
          code.trim().startsWith('stateDiagram') ||
          code.trim().startsWith('erDiagram') ||
          code.trim().startsWith('journey') ||
          code.trim().startsWith('gitGraph') ||
          code.trim().startsWith('pie') ||
          code.trim().startsWith('requirement')) {
        
        // Create a new div for the diagram
        const diagram = document.createElement('div');
        diagram.className = 'mermaid';
        diagram.textContent = code;
        
        // Replace the code block with the diagram
        pre.parentElement.replaceWith(diagram);
      }
    });
    
    // Render all mermaid diagrams
    if (window.mermaid && window.mermaid.contentLoaded) {
      mermaid.contentLoaded();
    }
  }
});

