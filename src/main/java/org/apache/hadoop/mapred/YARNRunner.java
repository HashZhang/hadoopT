package org.apache.hadoop.mapred;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenSelector;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class YARNRunner implements ClientProtocol {
    private static final Log LOG = LogFactory.getLog(YARNRunner.class);
    private final RecordFactory recordFactory;
    private ResourceMgrDelegate resMgrDelegate;
    private ClientCache clientCache;
    private Configuration conf;
    private final FileContext defaultFileContext;

    public YARNRunner(Configuration conf) {
        this(conf, new ResourceMgrDelegate(new YarnConfiguration(conf)));
    }

    public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate) {
        this(conf, resMgrDelegate, new ClientCache(conf, resMgrDelegate));
    }

    public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate, ClientCache clientCache) {
        this.recordFactory = RecordFactoryProvider.getRecordFactory((Configuration)null);
        this.conf = conf;

        try {
            this.resMgrDelegate = resMgrDelegate;
            this.clientCache = clientCache;
            this.defaultFileContext = FileContext.getFileContext(this.conf);
        } catch (UnsupportedFileSystemException var5) {
            throw new RuntimeException("Error in instantiating YarnClient", var5);
        }
    }

    @Private
    public void setResourceMgrDelegate(ResourceMgrDelegate resMgrDelegate) {
        this.resMgrDelegate = resMgrDelegate;
    }

    public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Use Token.renew instead");
    }

    public TaskTrackerInfo[] getActiveTrackers() throws IOException, InterruptedException {
        return this.resMgrDelegate.getActiveTrackers();
    }

    public JobStatus[] getAllJobs() throws IOException, InterruptedException {
        return this.resMgrDelegate.getAllJobs();
    }

    public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException, InterruptedException {
        return this.resMgrDelegate.getBlacklistedTrackers();
    }

    public ClusterMetrics getClusterMetrics() throws IOException, InterruptedException {
        return this.resMgrDelegate.getClusterMetrics();
    }

    @VisibleForTesting
    void addHistoryToken(Credentials ts) throws IOException, InterruptedException {
        MRClientProtocol hsProxy = this.clientCache.getInitializedHSProxy();
        if(UserGroupInformation.isSecurityEnabled() && hsProxy != null) {
            RMDelegationTokenSelector tokenSelector = new RMDelegationTokenSelector();
            Text service = this.resMgrDelegate.getRMDelegationTokenService();
            if(tokenSelector.selectToken(service, ts.getAllTokens()) != null) {
                Text hsService = SecurityUtil.buildTokenService(hsProxy.getConnectAddress());
                if(ts.getToken(hsService) == null) {
                    ts.addToken(hsService, this.getDelegationTokenFromHS(hsProxy));
                }
            }
        }

    }

    @VisibleForTesting
    Token<?> getDelegationTokenFromHS(MRClientProtocol hsProxy) throws IOException, InterruptedException {
        GetDelegationTokenRequest request = (GetDelegationTokenRequest)this.recordFactory.newRecordInstance(GetDelegationTokenRequest.class);
        request.setRenewer(Master.getMasterPrincipal(this.conf));
        org.apache.hadoop.yarn.api.records.Token mrDelegationToken = hsProxy.getDelegationToken(request).getDelegationToken();
        return ConverterUtils.convertFromYarn(mrDelegationToken, hsProxy.getConnectAddress());
    }

    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException, InterruptedException {
        return this.resMgrDelegate.getDelegationToken(renewer);
    }

    public String getFilesystemName() throws IOException, InterruptedException {
        return this.resMgrDelegate.getFilesystemName();
    }

    public JobID getNewJobID() throws IOException, InterruptedException {
        return this.resMgrDelegate.getNewJobID();
    }

    public QueueInfo getQueue(String queueName) throws IOException, InterruptedException {
        return this.resMgrDelegate.getQueue(queueName);
    }

    public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException, InterruptedException {
        return this.resMgrDelegate.getQueueAclsForCurrentUser();
    }

    public QueueInfo[] getQueues() throws IOException, InterruptedException {
        return this.resMgrDelegate.getQueues();
    }

    public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
        return this.resMgrDelegate.getRootQueues();
    }

    public QueueInfo[] getChildQueues(String parent) throws IOException, InterruptedException {
        return this.resMgrDelegate.getChildQueues(parent);
    }

    public String getStagingAreaDir() throws IOException, InterruptedException {
        return this.resMgrDelegate.getStagingAreaDir();
    }

    public String getSystemDir() throws IOException, InterruptedException {
        return this.resMgrDelegate.getSystemDir();
    }

    public long getTaskTrackerExpiryInterval() throws IOException, InterruptedException {
        return this.resMgrDelegate.getTaskTrackerExpiryInterval();
    }

    public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts) throws IOException, InterruptedException {
        this.addHistoryToken(ts);
        ApplicationSubmissionContext appContext = this.createApplicationSubmissionContext(this.conf, jobSubmitDir, ts);

        try {
            ApplicationId e = this.resMgrDelegate.submitApplication(appContext);
            ApplicationReport appMaster = this.resMgrDelegate.getApplicationReport(e);
            String diagnostics = appMaster == null?"application report is null":appMaster.getDiagnostics();
            if(appMaster != null && appMaster.getYarnApplicationState() != YarnApplicationState.FAILED && appMaster.getYarnApplicationState() != YarnApplicationState.KILLED) {
                return this.clientCache.getClient(jobId).getJobStatus(jobId);
            } else {
                throw new IOException("Failed to run job : " + diagnostics);
            }
        } catch (YarnException var8) {
            throw new IOException(var8);
        }
    }

    private LocalResource createApplicationResource(FileContext fs, Path p, LocalResourceType type) throws IOException {
        LocalResource rsrc = (LocalResource)this.recordFactory.newRecordInstance(LocalResource.class);
        FileStatus rsrcStat = fs.getFileStatus(p);
        rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs.getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
        rsrc.setSize(rsrcStat.getLen());
        rsrc.setTimestamp(rsrcStat.getModificationTime());
        rsrc.setType(type);
        rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        return rsrc;
    }

    public ApplicationSubmissionContext createApplicationSubmissionContext(Configuration jobConf, String jobSubmitDir, Credentials ts) throws IOException {
        ApplicationId applicationId = this.resMgrDelegate.getApplicationId();
        Resource capability = (Resource)this.recordFactory.newRecordInstance(Resource.class);
        capability.setMemory(this.conf.getInt("yarn.app.mapreduce.am.resource.mb", 1536));
        capability.setVirtualCores(this.conf.getInt("yarn.app.mapreduce.am.resource.cpu-vcores", 1));
        LOG.debug("AppMaster capability = " + capability);
        HashMap localResources = new HashMap();
        Path jobConfPath = new Path(jobSubmitDir, "job.xml");
        URL yarnUrlForJobSubmitDir = ConverterUtils.getYarnUrlFromPath(this.defaultFileContext.getDefaultFileSystem().resolvePath(this.defaultFileContext.makeQualified(new Path(jobSubmitDir))));
        LOG.debug("Creating setup context, jobSubmitDir url is " + yarnUrlForJobSubmitDir);
        localResources.put("job.xml", this.createApplicationResource(this.defaultFileContext, jobConfPath, LocalResourceType.FILE));
        if(jobConf.get("mapreduce.job.jar") != null) {
            Path dob = new Path(jobConf.get("mapreduce.job.jar"));
            LocalResource securityTokens = this.createApplicationResource(FileContext.getFileContext(dob.toUri(), jobConf), dob, LocalResourceType.PATTERN);
            String vargs = this.conf.getPattern("mapreduce.job.jar.unpack.pattern", JobConf.UNPACK_JAR_PATTERN_DEFAULT).pattern();
            securityTokens.setPattern(vargs);
            localResources.put("job.jar", securityTokens);
        } else {
            LOG.info("Job jar is not present. Not adding any jar to the list of resources.");
        }

        String[] var26 = new String[]{"job.split", "job.splitmetainfo"};
        int var29 = var26.length;

        for(int var28 = 0; var28 < var29; ++var28) {
            String amTmpDir = var26[var28];
            localResources.put("jobSubmitDir/" + amTmpDir, this.createApplicationResource(this.defaultFileContext, new Path(jobSubmitDir, amTmpDir), LocalResourceType.FILE));
        }

        DataOutputBuffer var27 = new DataOutputBuffer();
        ts.writeTokenStorageToStream(var27);
        ByteBuffer var30 = ByteBuffer.wrap(var27.getData(), 0, var27.getLength());
        ArrayList var32 = new ArrayList(8);
//        var32.add(MRApps.crossPlatformifyMREnv(jobConf, Environment.JAVA_HOME) + "/bin/java");
        var32.add("$JAVA_HOME/bin/java");
//        Path var31 = new Path(MRApps.crossPlatformifyMREnv(this.conf, Environment.PWD), "./tmp");
        Path var31 = new Path("$PWD/tmp");
        var32.add("-Djava.io.tmpdir=" + var31);
        MRApps.addLog4jSystemProperties((Task)null, var32, this.conf);
        warnForJavaLibPath(this.conf.get("mapreduce.map.java.opts", ""), "map", "mapreduce.map.java.opts", "mapreduce.map.env");
        warnForJavaLibPath(this.conf.get("mapreduce.admin.map.child.java.opts", ""), "map", "mapreduce.admin.map.child.java.opts", "mapreduce.admin.user.env");
        warnForJavaLibPath(this.conf.get("mapreduce.reduce.java.opts", ""), "reduce", "mapreduce.reduce.java.opts", "mapreduce.reduce.env");
        warnForJavaLibPath(this.conf.get("mapreduce.admin.reduce.child.java.opts", ""), "reduce", "mapreduce.admin.reduce.child.java.opts", "mapreduce.admin.user.env");
        String mrAppMasterAdminOptions = this.conf.get("yarn.app.mapreduce.am.admin-command-opts", "");
        warnForJavaLibPath(mrAppMasterAdminOptions, "app master", "yarn.app.mapreduce.am.admin-command-opts", "yarn.app.mapreduce.am.admin.user.env");
        var32.add(mrAppMasterAdminOptions);
        String mrAppMasterUserOptions = this.conf.get("yarn.app.mapreduce.am.command-opts", "-Xmx1024m");
        warnForJavaLibPath(mrAppMasterUserOptions, "app master", "yarn.app.mapreduce.am.command-opts", "yarn.app.mapreduce.am.env");
        var32.add(mrAppMasterUserOptions);
        if(jobConf.getBoolean("yarn.app.mapreduce.am.profile", false)) {
            String vargsFinal = jobConf.get("yarn.app.mapreduce.am.profile.params", "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s");
            if(vargsFinal != null) {
                var32.add(String.format(vargsFinal, new Object[]{"<LOG_DIR>/" + LogName.PROFILE}));
            }
        }

        var32.add("org.apache.hadoop.mapreduce.v2.app.MRAppMaster");
        var32.add("1><LOG_DIR>/stdout");
        var32.add("2><LOG_DIR>/stderr");
        Vector var33 = new Vector(8);
        StringBuilder mergedCommand = new StringBuilder();
        Iterator environment = var32.iterator();

        while(environment.hasNext()) {
            CharSequence acls = (CharSequence)environment.next();
            mergedCommand.append(acls).append(" ");
        }

        var33.add(mergedCommand.toString());
        LOG.debug("Command to launch container for ApplicationMaster is : " + mergedCommand);
        HashMap var34 = new HashMap();
        MRApps.setClasspath(var34, this.conf);
        var34.put(Environment.SHELL.name(), this.conf.get("mapreduce.admin.user.shell", "/bin/bash"));
        MRApps.addToEnvironment(var34, Environment.LD_LIBRARY_PATH.name(), MRApps.crossPlatformifyMREnv(this.conf, Environment.PWD), this.conf);
        MRApps.setEnvFromInputString(var34, this.conf.get("yarn.app.mapreduce.am.admin.user.env"), this.conf);
        MRApps.setEnvFromInputString(var34, this.conf.get("yarn.app.mapreduce.am.env"), this.conf);
        MRApps.setupDistributedCache(jobConf, localResources);
        HashMap var35 = new HashMap(2);
        var35.put(ApplicationAccessType.VIEW_APP, jobConf.get("mapreduce.job.acl-view-job", " "));
        var35.put(ApplicationAccessType.MODIFY_APP, jobConf.get("mapreduce.job.acl-modify-job", " "));
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, var34, var33, (Map)null, var30, var35);
        Collection tagsFromConf = jobConf.getTrimmedStringCollection("mapreduce.job.tags");
        ApplicationSubmissionContext appContext = (ApplicationSubmissionContext)this.recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
        appContext.setApplicationId(applicationId);
        appContext.setQueue(jobConf.get("mapreduce.job.queuename", "default"));
        ReservationId reservationID = null;

        try {
            reservationID = ReservationId.parseReservationId(jobConf.get("mapreduce.job.reservation.id"));
        } catch (NumberFormatException var25) {
            String errMsg = "Invalid reservationId: " + jobConf.get("mapreduce.job.reservation.id") + " specified for the app: " + applicationId;
            LOG.warn(errMsg);
            throw new IOException(errMsg);
        }

        if(reservationID != null) {
            appContext.setReservationID(reservationID);
            LOG.info("SUBMITTING ApplicationSubmissionContext app:" + applicationId + " to queue:" + appContext.getQueue() + " with reservationId:" + appContext.getReservationID());
        }

        appContext.setApplicationName(jobConf.get("mapreduce.job.name", "N/A"));
        appContext.setCancelTokensWhenComplete(this.conf.getBoolean("mapreduce.job.complete.cancel.delegation.tokens", true));
        appContext.setAMContainerSpec(amContainer);
        appContext.setMaxAppAttempts(this.conf.getInt("mapreduce.am.max-attempts", 2));
        appContext.setResource(capability);
        appContext.setApplicationType("MAPREDUCE");
        if(tagsFromConf != null && !tagsFromConf.isEmpty()) {
            appContext.setApplicationTags(new HashSet(tagsFromConf));
        }

        return appContext;
    }

    public void setJobPriority(JobID arg0, String arg1) throws IOException, InterruptedException {
        this.resMgrDelegate.setJobPriority(arg0, arg1);
    }

    public long getProtocolVersion(String arg0, long arg1) throws IOException {
        return this.resMgrDelegate.getProtocolVersion(arg0, arg1);
    }

    public long renewDelegationToken(Token<DelegationTokenIdentifier> arg0) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Use Token.renew instead");
    }

    public Counters getJobCounters(JobID arg0) throws IOException, InterruptedException {
        return this.clientCache.getClient(arg0).getJobCounters(arg0);
    }

    public String getJobHistoryDir() throws IOException, InterruptedException {
        return JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(this.conf);
    }

    public JobStatus getJobStatus(JobID jobID) throws IOException, InterruptedException {
        JobStatus status = this.clientCache.getClient(jobID).getJobStatus(jobID);
        return status;
    }

    public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1, int arg2) throws IOException, InterruptedException {
        return this.clientCache.getClient(arg0).getTaskCompletionEvents(arg0, arg1, arg2);
    }

    public String[] getTaskDiagnostics(TaskAttemptID arg0) throws IOException, InterruptedException {
        return this.clientCache.getClient(arg0.getJobID()).getTaskDiagnostics(arg0);
    }

    public TaskReport[] getTaskReports(JobID jobID, TaskType taskType) throws IOException, InterruptedException {
        return this.clientCache.getClient(jobID).getTaskReports(jobID, taskType);
    }

    private void killUnFinishedApplication(ApplicationId appId) throws IOException {
        ApplicationReport application = null;

        try {
            application = this.resMgrDelegate.getApplicationReport(appId);
        } catch (YarnException var4) {
            throw new IOException(var4);
        }

        if(application.getYarnApplicationState() != YarnApplicationState.FINISHED && application.getYarnApplicationState() != YarnApplicationState.FAILED && application.getYarnApplicationState() != YarnApplicationState.KILLED) {
            this.killApplication(appId);
        }
    }

    private void killApplication(ApplicationId appId) throws IOException {
        try {
            this.resMgrDelegate.killApplication(appId);
        } catch (YarnException var3) {
            throw new IOException(var3);
        }
    }

    private boolean isJobInTerminalState(JobStatus status) {
        return status.getState() == State.KILLED || status.getState() == State.FAILED || status.getState() == State.SUCCEEDED;
    }

    public void killJob(JobID arg0) throws IOException, InterruptedException {
        JobStatus status = this.clientCache.getClient(arg0).getJobStatus(arg0);
        ApplicationId appId = TypeConverter.toYarn(arg0).getAppId();
        if(status == null) {
            this.killUnFinishedApplication(appId);
        } else if(status.getState() != State.RUNNING) {
            this.killApplication(appId);
        } else {
            try {
                this.clientCache.getClient(arg0).killJob(arg0);
                long io = System.currentTimeMillis();
                long timeKillIssued = io;
                long killTimeOut = this.conf.getLong("yarn.app.mapreduce.am.hard-kill-timeout-ms", 10000L);

                while(io < timeKillIssued + killTimeOut && !this.isJobInTerminalState(status)) {
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException var11) {
                        break;
                    }

                    io = System.currentTimeMillis();
                    status = this.clientCache.getClient(arg0).getJobStatus(arg0);
                    if(status == null) {
                        this.killUnFinishedApplication(appId);
                        return;
                    }
                }
            } catch (IOException var12) {
                LOG.debug("Error when checking for application status", var12);
            }

            if(status != null && !this.isJobInTerminalState(status)) {
                this.killApplication(appId);
            }

        }
    }

    public boolean killTask(TaskAttemptID arg0, boolean arg1) throws IOException, InterruptedException {
        return this.clientCache.getClient(arg0.getJobID()).killTask(arg0, arg1);
    }

    public AccessControlList getQueueAdmins(String arg0) throws IOException {
        return new AccessControlList("*");
    }

    public JobTrackerStatus getJobTrackerStatus() throws IOException, InterruptedException {
        return JobTrackerStatus.RUNNING;
    }

    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
    }

    public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID) throws IOException {
        return this.clientCache.getClient(jobID).getLogFilePath(jobID, taskAttemptID);
    }

    private static void warnForJavaLibPath(String opts, String component, String javaConf, String envConf) {
        if(opts != null && opts.contains("-Djava.library.path")) {
            LOG.warn("Usage of -Djava.library.path in " + javaConf + " can cause " + "programs to no longer function if hadoop native libraries " + "are used. These values should be set as part of the " + "LD_LIBRARY_PATH in the " + component + " JVM env using " + envConf + " config settings.");
        }

    }
}
