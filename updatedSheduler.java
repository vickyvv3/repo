import org.apache.sling.api.resource.Resource;
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
import java.util.Calendar;
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
        
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, -6);
        Date targetDate = calendar.getTime(); 
        String formattedDate = DATE_FORMAT.format(new Date()); 

        LOG.info("Running scheduler. Today's date: {}", formattedDate);
        LOG.info("Target date (6 months ago): {}", DATE_FORMAT.format(targetDate));

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
                    processFolders(baseResource, output, resolver, session, targetPath, targetDate);
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

    /**
     * Processes child folders under the base path, and determines whether to move the entire folder
     * or only some nodes within the folder.
     */
    private void processFolders(Resource folder, StringBuilder output, ResourceResolver resolver, Session session, String targetPath, Date targetDate) throws RepositoryException {
        for (Resource childFolder : folder.getChildren()) {
            boolean allNodesOlderThanTargetDate = true;
            boolean anyNodeOlderThanTargetDate = false;

            // Check all nodes within the child folder
            for (Resource childNode : childFolder.getChildren()) {
                Resource contentResource = childNode.getChild("jcr:content");
                if (contentResource != null) {
                    ValueMap properties = contentResource.getValueMap();
                    String publishDateStr = properties.get("newsPublishDate", String.class);

                    if (publishDateStr != null) {
                        try {
                            Date publishDate = DATE_FORMAT.parse(publishDateStr);
                            if (publishDate.before(targetDate)) {
                                anyNodeOlderThanTargetDate = true; 
                            } else {
                                allNodesOlderThanTargetDate = false; 
                            }
                        } catch (Exception e) {
                            LOG.error("Failed to parse newsPublishDate for resource {}", childNode.getPath());
                        }
                    }
                }
            }

            String currentFolderPath = childFolder.getPath();
            String newFolderPath = targetPath + "/" + childFolder.getName();

            if (allNodesOlderThanTargetDate && anyNodeOlderThanTargetDate) {
                LOG.info("Moving entire folder from {} to {}", currentFolderPath, newFolderPath);
                session.move(currentFolderPath, newFolderPath);
            } else if (anyNodeOlderThanTargetDate) {
                LOG.info("Moving some nodes from folder {} to {}", currentFolderPath, newFolderPath);
                moveOldNodes(childFolder, newFolderPath, resolver, session, targetDate);
            } else {
                LOG.info("Skipping folder {} as no nodes are older than 6 months", currentFolderPath);
            }
            
            processFolders(childFolder, output, resolver, session, newFolderPath, targetDate);
        }
    }

    /**
     * Moves nodes older than 6 months from the folder to a target location.
     */
    private void moveOldNodes(Resource folder, String targetFolderPath, ResourceResolver resolver, Session session, Date targetDate) throws RepositoryException {
        for (Resource node : folder.getChildren()) {
            Resource contentResource = node.getChild("jcr:content");
            if (contentResource != null) {
                ValueMap properties = contentResource.getValueMap();
                String publishDateStr = properties.get("newsPublishDate", String.class);

                if (publishDateStr != null) {
                    try {
                        Date publishDate = DATE_FORMAT.parse(publishDateStr);
                        if (publishDate.before(targetDate)) {
                            String currentNodePath = node.getPath();
                            String newNodePath = targetFolderPath + "/" + node.getName();

                            LOG.info("Moving node from {} to {}", currentNodePath, newNodePath);
                            session.move(currentNodePath, newNodePath);
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to parse newsPublishDate for resource {}", node.getPath());
                    }
                }
            }
        }
    }

    @Activate
    protected void activate() {
        ScheduleOptions options = scheduler.EXPR("0 0 0 1 * ?");
        scheduler.schedule(this, options);
    }

    @Deactivate
    protected void deactivate() {
        scheduler.unschedule(this.getClass().getName());
    }
}
