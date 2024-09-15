import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.servlets.SlingAllMethodsServlet;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
        service = {javax.servlet.Servlet.class},
        property = {
                "sling.servlet.paths=/bin/moveData"
        }
)
public class MoveContentServlet extends SlingAllMethodsServlet {

    private static final Logger LOG = LoggerFactory.getLogger(MoveContentServlet.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    @Override
    protected void doGet(SlingHttpServletRequest request, SlingHttpServletResponse response) throws IOException {
        String timeFormat = "T00:00:00.000Z";
        String targetDateString = request.getParameter("targetDate")+timeFormat;
        StringBuilder logMessages = new StringBuilder();

        if (targetDateString == null) {
            logMessages.append("Missing targetDate parameter\n");
            response.setContentType("text/plain");
            response.getWriter().println(logMessages.toString());
            response.setStatus(SlingHttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        Date targetDate;
        try {
            targetDate = DATE_FORMAT.parse(targetDateString);
        } catch (ParseException e) {
            logMessages.append("Invalid targetDate format. Expected format is yyyy-MM-dd\n");
            response.setContentType("text/plain");
            response.getWriter().println(logMessages.toString());
            response.setStatus(SlingHttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String basePath = "/content/projects";
        String targetPath = "/content/site/us/en";

        ResourceResolver resolver = request.getResourceResolver();
        Session session = resolver.adaptTo(Session.class);

        Resource baseResource = resolver.getResource(basePath);

        if (baseResource != null) {
            try {
                movePages(baseResource, resolver, session, targetPath, targetDate, logMessages);
                session.save();
                logMessages.append("Content moved successfully from ").append(basePath).append(" to ").append(targetPath).append("\n");
            } catch (RepositoryException e) {
                logMessages.append("Error moving content: ").append(e.getMessage()).append("\n");
            }
        } else {
            logMessages.append("No resource found at ").append(basePath).append("\n");
        }

        response.setContentType("text/plain");
        response.getWriter().println(logMessages.toString());
    }

    private void movePages(Resource resource, ResourceResolver resolver, Session session, String targetPath, Date targetDate, StringBuilder logMessages) throws RepositoryException {
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

                            logMessages.append("Moving page from ").append(currentPath).append(" to ").append(newPath).append("\n");

                            try {
                                session.move(currentPath, newPath);
                            } catch (RepositoryException e) {
                                logMessages.append("Failed to move content at path: ").append(currentPath).append(". Error: ").append(e.getMessage()).append("\n");
                            }
                        } else {
                            logMessages.append("Skipping page: ").append(child.getPath()).append(" (newsPublishDate is newer than the target date)\n");
                        }
                    } catch (ParseException e) {
                        logMessages.append("Failed to parse newsPublishDate for resource ").append(child.getPath()).append(": ").append(e.getMessage()).append("\n");
                    }
                } else {
                    logMessages.append("No newsPublishDate found for resource ").append(child.getPath()).append("\n");
                }
            }

            movePages(child, resolver, session, targetPath + "/" + child.getName(), targetDate, logMessages);
        }
    }
}
