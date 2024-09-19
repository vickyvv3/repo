/**
     * The `run()` method is triggered by the scheduler.
     * It calculates the target date (6 months ago),
     * retrieves a `ResourceResolver`, and processes the nodes and folders under the base path.
     */
    @Override
    public void run() {

-----------------------------------------------------------------------------------------------

 /**
     * This method recursively processes child nodes and folders under a given resource.
     * It differentiates between pages (nodes) and folders.
     *
     * @param resource     the base resource to start from
     * @param resolver     the resource resolver
     * @param session      the JCR session
     * @param targetPath   the path where content will be moved
     * @param targetDate   the date used to compare node/folder age
     */
    private void movePagesAndNodes


-------------------------------------------------------------------------------------------
 /**
     * This method checks if a folder meets the criteria for moving based on the node's age and status.
     * It also checks if the folder already exists in the target path, deletes it if it does, and then moves it.
     *
     * @param folder       the folder resource to process
     * @param resolver     the resource resolver
     * @param session      the JCR session
     * @param targetPath   the target path where the folder will be moved
     * @param targetDate   the date threshold to compare against
     */
    private void moveFolderIfNeeded

-----------------------------------------------------------------------------------------------
 /**
     * This method moves an individual node if it is older than the target date.
     * It first checks if the node exists at the target path and deletes it before moving.
     *
     * @param node         the node resource to process
     * @param resolver     the resource resolver
     * @param session      the JCR session
     * @param targetPath   the path where the node will be moved
     * @param targetDate   the date used to check if the node should be moved
     */
    private void moveNodeIfOlder(

---------------------------------------------------
/**
     * The `activate()` method configures the scheduler to run the job at regular intervals.
     * This method is invoked when the OSGi component is activated.
     */
    @Activate
    protected void activate() {

-----------------------------------------
  /**
     * The `deactivate()` method stops the scheduler when the component is deactivated.
     */
    @Deactivate
    protected void deactivate() {
