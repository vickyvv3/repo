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
        final String basePath = "/content/site/us/en";
        final String targetPath = "/content/projects";
        final String itranslatePath = "/content/dam/projects/itranslate";

        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MONTH, -6);
        final Date targetDate = calendar.getTime();
        final String formattedTargetDate = DATE_FORMAT.format(targetDate);

        LOG.info("Current date: {}", DATE_FORMAT.format(new Date()));
        LOG.info("Target date (6 months ago): {}", formattedTargetDate);

        final Map<String, Object> param = new HashMap<>();
        param.put(ResourceResolverFactory.SUBSERVICE, "playserviceuser");

        try (final ResourceResolver resolver = resourceResolverFactory.getServiceResourceResolver(param)) {
            final Session session = resolver.adaptTo(Session.class);
            if (session == null) {
                LOG.error("Could not adapt ResourceResolver to Session");
                return;
            }

            final Resource baseResource = resolver.getResource(basePath);

            if (baseResource != null) {
                try {
                    movePagesAndNodes(baseResource, resolver, session, targetPath, itranslatePath, targetDate);
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

    private void movePagesAndNodes(final Resource resource, final ResourceResolver resolver, final Session session,
                                   final String targetPath, final String itranslatePath, final Date targetDate) throws RepositoryException {
        for (final Resource child : resource.getChildren()) {
            final Resource contentResource = child.getChild("jcr:content");
            if (contentResource != null) {
                moveNodeIfOlder(child, resolver, session, targetPath, itranslatePath, targetDate);
            } else {
                moveFolderIfNeeded(child, resolver, session, targetPath, itranslatePath, targetDate);
            }
        }
    }

    private void moveFolderIfNeeded(final Resource folder, final ResourceResolver resolver, final Session session,
                                    final String targetPath, final String itranslatePath, final Date targetDate) throws RepositoryException {
        final String folderPath = folder.getPath();
        final String folderName = folder.getName();
        boolean allNodesOlder = true;
        boolean hasOldNodes = false;
        final List<String> oldNodePaths = new ArrayList<>();

        for (final Resource node : folder.getChildren()) {
            final ValueMap prop = node.getValueMap();
            final String aemProjectStatus = prop.get("aemProjectStatus", String.class);
            LOG.info("aemProjectStatus: {}", aemProjectStatus);

            if ("COMPLETED".equals(aemProjectStatus)) {
                final ValueMap properties = node.getValueMap();
                final Calendar createdDate = properties.get("jcr:created", Calendar.class);

                if (createdDate != null) {
                    try {
                        final Date newsPublishDate = createdDate.getTime();
                        if (newsPublishDate.before(targetDate)) {
                            hasOldNodes = true;
                            oldNodePaths.add(node.getPath());
                        } else {
                            allNodesOlder = false;
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to parse jcr:created for resource {}", node.getPath());
                    }
                } else {
                    LOG.warn("No jcr:created found for resource {}", node.getPath());
                    allNodesOlder = false;
                }
            } else {
                LOG.info("Skipping node: {} (aemProjectStatus is not COMPLETED)", node.getPath());
            }
        }
//---
        // Pre-move check under "/content/dam/projects/itranslate"
        Resource itranslateFolderResource = resolver.getResource(itranslatePath + "/" + folderName);
        if (itranslateFolderResource != null) {
            // Delete the folder at "/content/dam/projects/itranslate"
            try {
                resolver.delete(itranslateFolderResource);
                LOG.info("Deleted folder at {}", itranslatePath + "/" + folderName);
            } catch (PersistenceException e) {
                LOG.error("Failed to delete folder at {}: {}", itranslatePath + "/" + folderName, e.getMessage());
            }
        }

        //here we check if the folder already exists in the target path
        Resource targetFolderResource = resolver.getResource(targetPath + "/" + folderName);
        if (targetFolderResource != null) {
            //here we check delete the folder at the target path before moving
            try {
                resolver.delete(targetFolderResource);
                LOG.info("Deleted existing folder at {}", targetPath + "/" + folderName);
            } catch (PersistenceException e) {
                LOG.error("Failed to delete folder at {}: {}", targetPath + "/" + folderName, e.getMessage());
            }
        }
//---
        if (allNodesOlder && !oldNodePaths.isEmpty()) {
            final String newFolderPath = targetPath + "/" + folderName;
            LOG.info("Moving entire folder from {} to {}", folderPath, newFolderPath);
            session.move(folderPath, newFolderPath);
        } else if (hasOldNodes) {
            final String targetFolderPath = targetPath + "/" + folderName;
            targetFolderResource = resolver.getResource(targetFolderPath);

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

            for (final String oldNodePath : oldNodePaths) {
                final String newNodePath = targetFolderPath + "/" + oldNodePath.substring(oldNodePath.lastIndexOf("/") + 1);
                LOG.info("Moving node from {} to {}", oldNodePath, newNodePath);
                session.move(oldNodePath, newNodePath);
            }
        }

        for (final Resource childFolder : folder.getChildren()) {
            if (childFolder.hasChildren()) {
                moveFolderIfNeeded(childFolder, resolver, session, targetPath + "/" + folderName, itranslatePath, targetDate);
            }
        }
    }

    private void moveNodeIfOlder(final Resource node, final ResourceResolver resolver, final Session session,
                                 final String targetPath, final String itranslatePath, final Date targetDate) throws RepositoryException {
        final ValueMap prop = node.getValueMap();
        final String aemProjectStatus = prop.get("aemProjectStatus", String.class);
        LOG.info("aemProjectStatus: {}", aemProjectStatus);

        if ("COMPLETED".equals(aemProjectStatus)) {
            final ValueMap properties = node.getValueMap();
            final Calendar createdDate = properties.get("jcr:created", Calendar.class);

            if (createdDate != null) {
                try {
                    final Date newsPublishDate = createdDate.getTime();
                    if (newsPublishDate.before(targetDate)) {
                        final String currentPath = node.getPath();
//---
                        //here before moving we check under "/content/dam/projects/itranslate"
                        Resource itranslateNodeResource = resolver.getResource(itranslatePath + "/" + node.getName());
                        if (itranslateNodeResource != null) {
                            try {
                                resolver.delete(itranslateNodeResource);
                                LOG.info("Deleted node at {}", itranslatePath + "/" + node.getName());
                            } catch (PersistenceException e) {
                                LOG.error("Failed to delete node at {}: {}", itranslatePath + "/" + node.getName(), e.getMessage());
                            }
                        }

                        //here we check if the node already exists in the target path
                        Resource targetNodeResource = resolver.getResource(targetPath + "/" + node.getName());
                        if (targetNodeResource != null) {
                            // Delete the existing node at the target path
                            try {
                                resolver.delete(targetNodeResource);
                                LOG.info("Deleted existing node at {}", targetPath + "/" + node.getName());
                            } catch (PersistenceException e) {
                                LOG.error("Failed to delete node at {}: {}", targetPath + "/" + node.getName(), e.getMessage());
                            }
                        }
//---
                        final String newPath = targetPath + "/" + node.getName();
                        LOG.info("Moving node from {} to {}", currentPath, newPath);
                        session.move(currentPath, newPath);
                    } else {
                        LOG.info("Skipping node: {} (newsPublishDate is newer than the target date)", node.getPath());
                    }
                } catch (Exception e) {
                    LOG.error("Failed to parse jcr:created for resource {}", node.getPath());
                }
            } else {
                LOG.warn("No jcr:created found for resource {}", node.getPath());
            }
        } else {
            LOG.info("Skipping node: {} (aemProjectStatus is not COMPLETED)", node.getPath());
        }
    }

    @Activate
    protected void activate() {
        final ScheduleOptions options = scheduler.EXPR("0 0/2 * 1/1 * ? *");
        scheduler.schedule(this, options);
    }

    @Deactivate
    protected void deactivate() {
        scheduler.unschedule(this.getClass().getName());
    }
}
