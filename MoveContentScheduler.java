mport org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.commons.scheduler.ScheduleOptions;
import org.apache.sling.commons.scheduler.Scheduler;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component(
        service = Runnable.class,
        immediate = true
)
public class MoveContentScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MoveContentScheduler.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    @Reference
    private Scheduler scheduler;

    @Override
    public void run() {
        String basePath = "/content/site/us/en";
        String targetPath = "/content/projects";

        Date targetDate = new Date();
        String formattedDate = DATE_FORMAT.format(targetDate);

        LOG.info("Current date: {}", formattedDate);

        Map<String, Object> param = new HashMap<>();
        param.put(ResourceResolverFactory.SUBSERVICE, "playserviceuser");

        try (ResourceResolver resolver = resourceResolverFactory.getServiceResourceResolver(param)) {
            Session session = resolver.adaptTo(Session.class);
            if (session == null) {
                LOG.error("Could not adapt ResourceResolver to Session");
                return;
            }

            Resource baseResource = resolver.getResource(basePath);
            StringBuilder output = new StringBuilder();

            if (baseResource != null) {
                try {
                    movePages(baseResource, output, resolver, session, targetPath, targetDate);
                    session.save();
                    LOG.info("Content moved successfully from {} to {}", basePath, targetPath);
                } catch (RepositoryException e) {
                    LOG.error("Error moving content: {}", e.getMessage());
                }
            } else {
                LOG.warn("No resource found at {}", basePath);
            }

        } catch (Exception e) {
            LOG.error("Error obtaining resource resolver", e);
        }
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

                            LOG.info("Moving page from {} to {}", currentPath, newPath);
                            session.move(currentPath, newPath);
                        } else {
                            LOG.info("Skipping page: {} (newsPublishDate is newer than the target date)", child.getPath());
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to parse newsPublishDate for resource {}", child.getPath());
                    }
                } else {
                    LOG.warn("No newsPublishDate found for resource {}", child.getPath());
                }
            }

            movePages(child, output, resolver, session, targetPath + "/" + child.getName(), targetDate);
        }
    }

    @Activate
    protected void activate() {
        ScheduleOptions options = scheduler.EXPR("0 0/2 * 1/1 * ? *");
        scheduler.schedule(this, options);
    }

    @Deactivate
    protected void deactivate() {
        scheduler.unschedule(this.getClass().getName());
    }
}
