package apps.acs_002dtools.components.aemfiddle.fiddle;

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.servlets.SlingAllMethodsServlet;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.PersistenceException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.IOException;
import java.io.PrintWriter;

public class fiddle extends SlingAllMethodsServlet {

    @Override
    protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response) throws IOException {
        String basePath = "/content/projects/test";  
        String targetPath = "/var/test-var";  // Target path

        ResourceResolver resolver = request.getResourceResolver();
        Session session = resolver.adaptTo(Session.class);
        StringBuilder output = new StringBuilder();

        Resource baseResource = resolver.getResource(basePath);

        if (baseResource != null) {
            try {
                movePages(baseResource, output, resolver, session, targetPath);
                session.save(); 
                output.append("Content moved successfully from ").append(basePath).append(" to ").append(targetPath);
            } catch (RepositoryException e) {
                output.append("Error moving content: ").append(e.getMessage());
            }
        } else {
            output.append("No resource found at ").append(basePath);
        }

        response.setContentType("text/plain");
        PrintWriter writer = response.getWriter();
        writer.println(output.toString());
    }

    private void movePages(Resource resource, StringBuilder output, ResourceResolver resolver, Session session, String targetPath) throws RepositoryException {
        for (Resource child : resource.getChildren()) {
            String currentPath = child.getPath();

            
            if (currentPath.endsWith("/jcr:content")) {
                currentPath = currentPath.substring(0, currentPath.length() - "/jcr:content".length());
            }

            
            String newPath = targetPath + "/" + child.getName();
            output.append("Moving content from ").append(currentPath).append(" to ").append(newPath).append("\n");

            try {
                session.move(currentPath, newPath); 
            } catch (RepositoryException e) {
            }

            // Recursively move child resources if there are any
            movePages(child, output, resolver, session, targetPath + "/" + child.getName());
        }
    }
}
