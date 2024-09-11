package apps.acs_002dtools.components.aemfiddle.fiddle;

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.servlets.SlingAllMethodsServlet;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ValueMap; 
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class fiddle extends SlingAllMethodsServlet {

    private static final String TARGET_DATE_STRING = "2024-04-11T00:00:00.000Z";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    @Override
    protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response) throws IOException {
        String basePath = "/content/<orginal path>";  
        String targetPath = "/content/<archive path>";  

        ResourceResolver resolver = request.getResourceResolver();
        Session session = resolver.adaptTo(Session.class);
        StringBuilder output = new StringBuilder();

        Resource baseResource = resolver.getResource(basePath);

        if (baseResource != null) {
            try {
                Date targetDate = DATE_FORMAT.parse(TARGET_DATE_STRING);
                movePages(baseResource, output, resolver, session, targetPath, targetDate);
                session.save();
                output.append("Content moved successfully from ").append(basePath).append(" to ").append(targetPath);
            } catch (RepositoryException | ParseException e) {
                output.append("Error moving content: ").append(e.getMessage());
            }
        } else {
            output.append("No resource found at ").append(basePath);
        }

        response.setContentType("text/plain");
        PrintWriter writer = response.getWriter();
        writer.println(output.toString());
    }

    private void movePages(Resource resource, StringBuilder output, ResourceResolver resolver, Session session, String targetPath, Date targetDate) throws RepositoryException {
        for (Resource child : resource.getChildren()) {
            Resource contentResource = child.getChild("jcr:content");
            if (contentResource != null) {
                ValueMap properties = contentResource.getValueMap(); 
                String newsPublishDateStr = properties.get("newsPublishDate", String.class);

                if (newsPublishDateStr != null) {
                    try {
                        Date newsPublishDate = DATE_FORMAT.parse(newsPublishDateStr);
                        if (newsPublishDate.before(targetDate)) {
                            String currentPath = child.getPath();
                            String newPath = targetPath + "/" + child.getName();

                            output.append("Moving page from ").append(currentPath).append(" to ").append(newPath).append("\n");

                           
                            try {
                                session.move(currentPath, newPath);
                            } catch (RepositoryException e) {
                                output.append("Failed to move content at path: ").append(currentPath).append(". Error: ").append(e.getMessage()).append("\n");
                            }
                        } else {
                            output.append("Skipping page: ").append(child.getPath()).append(" (newsPublishDate is newer than the target date)\n");
                        }
                    } catch (ParseException e) {
                        output.append("Failed to parse newsPublishDate for resource ").append(child.getPath()).append("\n");
                    }
                } else {
                    output.append("No newsPublishDate found for resource ").append(child.getPath()).append("\n");
                }
            }

            movePages(child, output, resolver, session, targetPath + "/" + child.getName(), targetDate);
        }
    }
}
