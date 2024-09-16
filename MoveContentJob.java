
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.scheduler.Job;
import org.apache.sling.commons.scheduler.JobContext;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component(
        service = Job.class,
        property = {
                "scheduler.expression=*/2 * * * * ?",
                "scheduler.concurrent=false"
        }
)
public class MoveContentJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(MoveContentJob.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    @Override
    public void execute(JobContext context) {
        String basePath = "/content/projects";
        String targetPath = "/content/site/us/en";
        String targetDateString = new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "T00:00:00.000Z";
        Date targetDate;

        try {
            targetDate = DATE_FORMAT.parse(targetDateString);
        } catch (ParseException e) {
            LOG.error("Invalid targetDate format. Expected format is yyyy-MM-dd'T'HH:mm:ss.SSSX", e);
            return;
        }

        Map<String, Object> param = new HashMap<>();
        param.put(ResourceResolverFactory.SUBSERVICE, "dataMoverServiceUser");

        try (ResourceResolver resolver = resourceResolverFactory.getServiceResourceResolver(param)) {
            Session session = resolver.adaptTo(Session.class);
            Resource baseResource = resolver.getResource(basePath);

            if (baseResource != null) {
                try {
                    movePages(baseResource, resolver, session, targetPath, targetDate);
                    session.save();
                    LOG.info("Content moved successfully from {} to {}", basePath, targetPath);
                } catch (RepositoryException e) {
                    LOG.error("Error moving content: {}", e.getMessage(), e);
                }
            } else {
                LOG.warn("No resource found at {}", basePath);
            }
        } catch (Exception e) {
            LOG.error("Error getting resource resolver", e);
        }
    }

    private void movePages(Resource resource, ResourceResolver resolver, Session session, String targetPath, Date targetDate) throws RepositoryException {
        for (Resource child : resource.getChildren()) {
            Resource contentResource = child.getChild("jcr:content");
            if (contentResource != null) {
                String newsPublishDateStr = contentResource.getValueMap().get("newsPublishDate", String.class);

                if (newsPublishDateStr != null) {
                    try {
                        Date newsPublishDate = DATE_FORMAT.parse(newsPublishDateStr);
                        if (newsPublishDate.before(targetDate)) {
                            String currentPath = child.getPath();
                            String newPath = targetPath + "/" + child.getName();

                            LOG.info("Moving page from {} to {}", currentPath, newPath);

                            try {
                                session.move(currentPath, newPath);
                            } catch (RepositoryException e) {
                                LOG.error("Failed to move content at path: {}. Error: {}", currentPath, e.getMessage(), e);
                            }
                        } else {
                            LOG.info("Skipping page: {} (newsPublishDate is newer than the target date)", child.getPath());
                        }
                    } catch (ParseException e) {
                        LOG.error("Failed to parse newsPublishDate for resource {}: {}", child.getPath(), e.getMessage(), e);
                    }
                } else {
                    LOG.warn("No newsPublishDate found for resource {}", child.getPath());
                }
            }

            movePages(child, resolver, session, targetPath + "/" + child.getName(), targetDate);
        }
    }
}
