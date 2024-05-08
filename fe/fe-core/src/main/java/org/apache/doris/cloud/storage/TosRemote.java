// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.cloud.storage;

import org.apache.doris.common.DdlException;

import com.volcengine.model.request.AssumeRoleRequest;
import com.volcengine.model.response.AssumeRoleResponse;
import com.volcengine.model.response.AssumeRoleResponse.Credentials;
import com.volcengine.service.sts.ISTSService;
import com.volcengine.service.sts.impl.STSServiceImpl;
import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TOSV2ClientBuilder;
import com.volcengine.tos.TosClientException;
import com.volcengine.tos.comm.HttpMethod;
import com.volcengine.tos.model.object.PreSignedURLInput;
import com.volcengine.tos.model.object.PreSignedURLOutput;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TosRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(TosRemote.class);

    public TosRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        String region = obj.getRegion();
        String endPoint = obj.getEndpoint();
        String accessKey = obj.getAk();
        String secretKey = obj.getSk();
        // Tos have two endpoint: tos endpoint and s3 endpoint like:
        // tos endpoint: tos-cn-beijing.ivolces.com
        // s3  endpoint: tos-s3-cn-beijing.ivolces.com
        if (endPoint.contains("s3-")) {
            endPoint = endPoint.replaceFirst("s3-", "");
        }
        TOSV2 client = new TOSV2ClientBuilder().build(region, endPoint, accessKey, secretKey);

        String bucketName = obj.getBucket();
        String objectName = normalizePrefix(fileName);
        // default 1 hour.
        long expires = 3600;
        String signedUrl = "";
        try {
            PreSignedURLInput input = new PreSignedURLInput();
            input.setBucket(bucketName);
            input.setKey(objectName);
            input.setHttpMethod(HttpMethod.PUT);
            input.setExpires(expires);
            PreSignedURLOutput output = client.preSignedURL(input);
            signedUrl = output.getSignedUrl();
        } catch (TosClientException e) {
            LOG.error("preSignedURL failed", e);
        } catch (Throwable t) {
            LOG.error("unexpected exception in preSignedURL", t);
        }

        LOG.info("tos temporary signature url: {}", signedUrl);
        return signedUrl;
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        ISTSService stsService = STSServiceImpl.getInstance();
        String accessKey = obj.getAk();
        String secretKey = obj.getSk();
        stsService.setAccessKey(accessKey);
        stsService.setSecretKey(secretKey);

        AssumeRoleRequest request = new AssumeRoleRequest();
        request.setRoleSessionName(getNewRoleSessionName());
        request.setDurationSeconds(getDurationSeconds());
        request.setRoleTrn(obj.getArn());

        AssumeRoleResponse resp = null;
        try {
            resp = stsService.assumeRole(request);
        } catch (Exception e) {
            LOG.error("getSts token failed", e);
            throw new DdlException("getSts token failed", e);
        }
        Credentials credentials = resp.getResult().getCredentials();
        return Triple.of(credentials.getAccessKeyId(), credentials.getSecretAccessKey(), credentials.getSessionToken());
    }

    @Override
    public String toString() {
        return "TosRemote{obj=" + obj + '}';
    }
}
