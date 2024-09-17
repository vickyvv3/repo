import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
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
import java.util.*;

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
        String formattedTargetDate = DATE_FORMAT.format(targetDate);

        LOG.info("Current date: {}", DATE_FORMAT.format(new Date()));
        LOG.info("Target date (6 months ago): {}", formattedTargetDate);

        Map<String, Object> param = new HashMap<>();
        param.put(ResourceResolverFactory.SUBSERVICE, "playserviceuser");

        try (ResourceResolver resolver = resourceResolverFactory.getServiceResourceResolver(param)) {
            Session session = resolver.adaptTo(Session.class);
            if (session == null) {
                LOG.error("Could not adapt ResourceResolver to Session");
                return;
            }

            Resource baseResource = resolver.getResource(basePath);

            if (baseResource != null) {
                try {
                    movePagesAndNodes(baseResource, resolver, session, targetPath, targetDate);
                    session.save();
                    LOG.info("Content moved successfully from {} to {}", basePath, targetPath);
                } catch (RepositoryException e) {
                    LOG.error("Error moving content: {}", e.getMessage());
                }
            } else {
                LOG.warn("No resource found at {}", basePath);
            }

        } catch (LoginException e) {
            LOG.error("Error obtaining resource resolver", e);
        }
    }

    private void movePagesAndNodes(Resource resource, ResourceResolver resolver, Session session,
                                   String targetPath, Date targetDate) throws RepositoryException {
        for (Resource child : resource.getChildren()) {
            Resource contentResource = child.getChild("jcr:content");
            if (contentResource != null) {
                moveNodeIfOlder(child, resolver, session, targetPath, targetDate);
            } else {
                moveFolderIfNeeded(child, resolver, session, targetPath, targetDate);
            }
        }
    }

    private void moveFolderIfNeeded(Resource folder, ResourceResolver resolver, Session session,
                                    String targetPath, Date targetDate) throws RepositoryException {
        String folderPath = folder.getPath();
        String folderName = folder.getName();
        boolean allNodesOlder = true;
        boolean hasOldNodes = false;
        List<String> oldNodePaths = new ArrayList<>();

        for (Resource node : folder.getChildren()) {
            Resource contentResource = node.getChild("jcr:content");
            if (contentResource != null) {
                ValueMap properties = contentResource.getValueMap();
                String newsPublishDateStr = properties.get("newsPublishDate", String.class);

                if (newsPublishDateStr != null) {
                    try {
                        Date newsPublishDate = DATE_FORMAT.parse(newsPublishDateStr);
                        if (newsPublishDate.before(targetDate)) {
                            hasOldNodes = true;
                            oldNodePaths.add(node.getPath());
                        } else {
                            allNodesOlder = false;
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to parse newsPublishDate for resource {}", node.getPath());
                    }
                } else {
                    LOG.warn("No newsPublishDate found for resource {}", node.getPath());
                    allNodesOlder = false;
                }
            }
        }

        if (allNodesOlder && !oldNodePaths.isEmpty()) {
            String newFolderPath = targetPath + "/" + folderName;
            LOG.info("Moving entire folder from {} to {}", folderPath, newFolderPath);
            session.move(folderPath, newFolderPath);
        } else if (hasOldNodes) {
            String targetFolderPath = targetPath + "/" + folderName;
            Resource targetFolderResource = resolver.getResource(targetFolderPath);

            if (targetFolderResource == null) {
                try {
                    resolver.create(resolver.getResource(targetPath), folderName, null);
                    LOG.info("Created folder at {}", targetFolderPath);
                } catch (PersistenceException e) {
                    LOG.error("Failed to create folder at {}: {}", targetFolderPath, e.getMessage());
                }
            } else {
                LOG.info("Folder already exists at {}, continuing to move old nodes", targetFolderPath);
            }

            for (String oldNodePath : oldNodePaths) {
                String newNodePath = targetFolderPath + "/" + oldNodePath.substring(oldNodePath.lastIndexOf("/") + 1);
                LOG.info("Moving node from {} to {}", oldNodePath, newNodePath);
                session.move(oldNodePath, newNodePath);
            }
        }

        for (Resource childFolder : folder.getChildren()) {
            if (childFolder.hasChildren()) {
                moveFolderIfNeeded(childFolder, resolver, session, targetPath + "/" + folderName, targetDate);
            }
        }
    }

    private void moveNodeIfOlder(Resource node, ResourceResolver resolver, Session session,
                                 String targetPath, Date targetDate) throws RepositoryException {
        Resource contentResource = node.getChild("jcr:content");
        if (contentResource != null) {
            ValueMap properties = contentResource.getValueMap();
            String newsPublishDateStr = properties.get("newsPublishDate", String.class);

            if (newsPublishDateStr != null) {
                try {
                    Date newsPublishDate = DATE_FORMAT.parse(newsPublishDateStr);
                    if (newsPublishDate.before(targetDate)) {
                        String currentPath = node.getPath();
                        String newPath = targetPath + "/" + node.getName();
                        LOG.info("Moving node from {} to {}", currentPath, newPath);
                        session.move(currentPath, newPath);
                    } else {
                        LOG.info("Skipping node: {} (newsPublishDate is newer than the target date)", node.getPath());
                    }
                } catch (Exception e) {
                    LOG.error("Failed to parse newsPublishDate for resource {}", node.getPath());
                }
            } else {
                LOG.warn("No newsPublishDate found for resource {}", node.getPath());
            }
        }
    }

    @Activate
    protected void activate() {
        ScheduleOptions options = scheduler.EXPR("\n" +
                "0 0/2 * 1/1 * ? *");
        scheduler.schedule(this, options);
    }

    @Deactivate
    protected void deactivate() {
        scheduler.unschedule(this.getClass().getName());
    }
}
